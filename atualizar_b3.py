#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ATUALIZADOR AUTOMÁTICO B3 - GOOGLE BIGQUERY
================================================================================
Projeto: Pipeline de Dados B3 (Mercado Brasileiro) & Power BI
Responsável: Karl Albert / Engenharia de Dados & BI
Destino: Google BigQuery (Dataset: B3)
Tabelas:
  - fechamento_tickers (Fato: Cotações diárias/intraday de ações da B3)
  - fechamento_ibov    (Fato: Pontos, máximas, mínimas e volume do Ibovespa)
  - fechamento_dolar   (Fato: Cotação USD/BRL PTAX / Fechamento)
  - ativos_board       (Dimensão: Ativos monitorados, setores e status)
================================================================================
"""

import os
import sys
import json
import logging
from datetime import datetime, date, timedelta
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuração de Logging com formato profissional
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("Atualizador_B3")

# ==============================================================================
# CONFIGURAÇÕES E VARIÁVEIS DE AMBIENTE
# ==============================================================================
GCP_SA_KEY = os.environ.get("GCP_SA_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "project-1c5de651-f9e1-439e-854")
DATASET_ID = os.environ.get("DATASET_ID", "B3")

LUNN_API_URL = os.environ.get("LUNN_API_URL")
LUNN_API_KEY = os.environ.get("LUNN_API_KEY")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# ==============================================================================
# 1. CLIENTE BIGQUERY
# ==============================================================================
def obter_cliente_bigquery():
    """Inicializa o cliente do Google BigQuery via Service Account ou ADC."""
    try:
        if GCP_SA_KEY:
            try:
                # Tenta interpretar como JSON string
                sa_info = json.loads(GCP_SA_KEY)
                credentials = service_account.Credentials.from_service_account_info(sa_info)
                client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
                logger.info(f"Conectado ao BigQuery com Service Account no projeto '{GCP_PROJECT_ID}'.")
                return client
            except json.JSONDecodeError:
                # Se for caminho de arquivo
                if os.path.exists(GCP_SA_KEY):
                    credentials = service_account.Credentials.from_service_account_file(GCP_SA_KEY)
                    client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
                    logger.info(f"Conectado ao BigQuery via arquivo de credenciais no projeto '{GCP_PROJECT_ID}'.")
                    return client
        
        # Fallback para credenciais padrão do ambiente (gcloud auth)
        client = bigquery.Client(project=GCP_PROJECT_ID)
        logger.info(f"Conectado ao BigQuery via Application Default Credentials (ADC) no projeto '{GCP_PROJECT_ID}'.")
        return client
    except Exception as e:
        logger.error(f"Erro crítico ao inicializar cliente do BigQuery: {e}")
        raise e


def garantir_dataset_e_tabelas(client: bigquery.Client):
    """Garante que o dataset e as tabelas existam com particionamento adequado."""
    dataset_ref = f"{GCP_PROJECT_ID}.{DATASET_ID}"
    
    # Criar dataset se não existir
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset '{dataset_ref}' validado.")
    except Exception:
        logger.info(f"Criando dataset '{dataset_ref}'...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset '{dataset_ref}' criado com sucesso.")

    # 1. Tabela fechamento_tickers (Particionada por data e clusterizada por ticker)
    tabela_tickers = f"{dataset_ref}.fechamento_tickers"
    schema_tickers = [
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("preco", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("variacao", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("dy", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("p_vp", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("volume", "INT64", mode="NULLABLE"),
    ]
    try:
        client.get_table(tabela_tickers)
    except Exception:
        table = bigquery.Table(tabela_tickers, schema=schema_tickers)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data"
        )
        table.clustering_fields = ["ticker"]
        client.create_table(table, exists_ok=True)
        logger.info(f"Tabela particionada '{tabela_tickers}' criada.")

    # 2. Tabela fechamento_ibov
    tabela_ibov = f"{dataset_ref}.fechamento_ibov"
    schema_ibov = [
        bigquery.SchemaField("data", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("abertura", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("maxima", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("minima", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("fechamento", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("ultimo", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("variacao", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("volume", "INT64", mode="NULLABLE"),
    ]
    try:
        client.get_table(tabela_ibov)
    except Exception:
        table = bigquery.Table(tabela_ibov, schema=schema_ibov)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data"
        )
        client.create_table(table, exists_ok=True)
        logger.info(f"Tabela '{tabela_ibov}' criada.")

    # 3. Tabela fechamento_dolar
    tabela_dolar = f"{dataset_ref}.fechamento_dolar"
    schema_dolar = [
        bigquery.SchemaField("data", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("compra", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("venda", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("maxima", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("minima", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("variacao", "FLOAT64", mode="NULLABLE"),
    ]
    try:
        client.get_table(tabela_dolar)
    except Exception:
        table = bigquery.Table(tabela_dolar, schema=schema_dolar)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data"
        )
        client.create_table(table, exists_ok=True)
        logger.info(f"Tabela '{tabela_dolar}' criada.")

    # 4. Tabela ativos_board (Dimensão)
    tabela_ativos = f"{dataset_ref}.ativos_board"
    schema_ativos = [
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("nome_empresa", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("setor_atuacao", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ticker_inativo", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("destino", "STRING", mode="NULLABLE"),
    ]
    try:
        client.get_table(tabela_ativos)
    except Exception:
        table = bigquery.Table(tabela_ativos, schema=schema_ativos)
        client.create_table(table, exists_ok=True)
        logger.info(f"Tabela '{tabela_ativos}' criada.")


# ==============================================================================
# 2. EXTRAÇÃO DE COTAÇÕES DA B3 (API LUNN / SUPABASE / YAHOO FINANCE FALLBACK)
# ==============================================================================
def extrair_cotacoes_b3() -> pd.DataFrame:
    """Extrai cotações da B3 via API LUNN, Supabase ou Yahoo Finance."""
    # 1. Tentar API LUNN se configurada
    if LUNN_API_URL:
        try:
            logger.info(f"Consultando API LUNN em: {LUNN_API_URL}...")
            headers = {"Authorization": f"Bearer {LUNN_API_KEY}"} if LUNN_API_KEY else {}
            resp = requests.get(LUNN_API_URL, headers=headers, timeout=60)
            if resp.status_code == 200:
                dados = resp.json()
                df = pd.DataFrame(dados)
                if not df.empty:
                    logger.info(f"Sucesso na API LUNN: {len(df)} cotações obtidas.")
                    return normalizar_df_tickers(df)
            logger.warning(f"API LUNN retornou status {resp.status_code}. Tentando canal alternativo...")
        except Exception as e:
            logger.warning(f"Falha ao consultar API LUNN: {e}")

    # 2. Tentar Supabase intermediário se configurado
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            logger.info("Buscando cotações no Supabase (tabela fechamento_tickers)...")
            url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/fechamento_tickers?select=*&order=data.desc&limit=5000"
            headers = {
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}"
            }
            resp = requests.get(url, headers=headers, timeout=60)
            if resp.status_code == 200:
                dados = resp.json()
                df = pd.DataFrame(dados)
                if not df.empty:
                    logger.info(f"Sucesso no Supabase: {len(df)} cotações obtidas.")
                    return normalizar_df_tickers(df)
        except Exception as e:
            logger.warning(f"Falha ao consultar Supabase: {e}")

    # 3. Fallback inteligente: Yahoo Finance para principais ações da B3 (.SA)
    logger.info("Utilizando fallback automático via Yahoo Finance para mercado brasileiro (.SA)...")
    return extrair_b3_via_yfinance()


def normalizar_df_tickers(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza tipos e colunas da tabela de tickers."""
    # Renomear colunas se vierem com nomes em inglês ou maiúsculas
    col_map = {
        "Ticker": "ticker", "TICKER": "ticker",
        "Data": "data", "DATA": "data", "date": "data",
        "Preco": "preco", "PRECO": "preco", "close": "preco", "price": "preco",
        "Variacao": "variacao", "VARIACAO": "variacao", "change": "variacao",
        "DY": "dy", "dy_val": "dy", "dividend_yield": "dy",
        "P_VP": "p_vp", "pvp": "p_vp", "price_to_book": "p_vp",
        "Volume": "volume", "VOLUME": "volume"
    }
    df = df.rename(columns=col_map)
    
    # Garantir colunas obrigatórias
    for col in ["ticker", "data", "preco", "variacao", "dy", "p_vp", "volume"]:
        if col not in df.columns:
            df[col] = None

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["data"] = pd.to_datetime(df["data"]).dt.date
    df["preco"] = pd.to_numeric(df["preco"], errors="coerce")
    df["variacao"] = pd.to_numeric(df["variacao"], errors="coerce")
    df["dy"] = pd.to_numeric(df["dy"], errors="coerce").fillna(0.0)
    df["p_vp"] = pd.to_numeric(df["p_vp"], errors="coerce").fillna(0.0)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

    # Remover linhas sem ticker ou sem data
    df = df.dropna(subset=["ticker", "data"])
    # Remover duplicatas no lote
    df = df.drop_duplicates(subset=["ticker", "data"], keep="last")
    return df[["ticker", "data", "preco", "variacao", "dy", "p_vp", "volume"]]


def extrair_b3_via_yfinance() -> pd.DataFrame:
    """Coleta cotações diárias das principais empresas da B3 via yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance não instalado.")
        return pd.DataFrame()

    tickers_b3 = [
        "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "BBAS3.SA", "ABEV3.SA",
        "WEGE3.SA", "RENT3.SA", "SUZB3.SA", "GGBR4.SA", "JBSS3.SA", "RAIL3.SA",
        "EQTL3.SA", "LREN3.SA", "RADL3.SA", "PRIO3.SA", "RDOR3.SA", "VIVT3.SA",
        "EMBR3.SA", "CSNA3.SA", "CPLE6.SA", "CMIG4.SA", "SBSP3.SA", "TIMS3.SA",
        "UGPA3.SA", "HAPV3.SA", "ASAI3.SA", "CRFB3.SA", "CCRO3.SA", "ELET3.SA",
        "ELET6.SA", "EGIE3.SA", "KLBN11.SA", "MULT3.SA", "CYRE3.SA", "MRVE3.SA",
        "TOTS3.SA", "AZUL4.SA", "CVCB3.SA", "MGLU3.SA", "BHIA3.SA", "B3SA3.SA"
    ]
    
    logger.info(f"Baixando {len(tickers_b3)} ativos da B3 via Yahoo Finance (janela 5d)...")
    try:
        data = yf.download(tickers_b3, period="5d", interval="1d", group_by="ticker", auto_adjust=False, threads=True)
    except Exception as e:
        logger.error(f"Erro no download yfinance: {e}")
        return pd.DataFrame()

    registros = []
    for t in tickers_b3:
        clean_ticker = t.replace(".SA", "")
        try:
            if clean_ticker in data or t in data:
                sub_df = data[t] if t in data else data[clean_ticker]
                sub_df = sub_df.dropna(subset=["Close"])
                for idx, row in sub_df.iterrows():
                    d_dt = idx.date()
                    preco = float(row["Close"])
                    open_p = float(row["Open"]) if pd.notnull(row["Open"]) else preco
                    var = ((preco - open_p) / open_p) * 100 if open_p > 0 else 0.0
                    vol = int(row["Volume"]) if pd.notnull(row["Volume"]) else 0
                    registros.append({
                        "ticker": clean_ticker,
                        "data": d_dt,
                        "preco": round(preco, 2),
                        "variacao": round(var, 2),
                        "dy": 0.0,
                        "p_vp": 0.0,
                        "volume": vol
                    })
        except Exception:
            continue

    df = pd.DataFrame(registros)
    return normalizar_df_tickers(df)


# ==============================================================================
# 3. EXTRAÇÃO DO IBOVESPA E DÓLAR
# ==============================================================================
def extrair_fechamento_ibov() -> pd.DataFrame:
    """Obtém os últimos pontos e volume do Ibovespa."""
    try:
        import yfinance as yf
        logger.info("Extraindo dados do índice Ibovespa (^BVSP)...")
        ibov = yf.Ticker("^BVSP")
        hist = ibov.history(period="5d", interval="1d")
        if hist.empty:
            return pd.DataFrame()
        
        registros = []
        for idx, row in hist.iterrows():
            d_dt = idx.date()
            abertura = round(float(row["Open"]), 2)
            maxima = round(float(row["High"]), 2)
            minima = round(float(row["Low"]), 2)
            fechamento = round(float(row["Close"]), 2)
            var = round(((fechamento - abertura) / abertura) * 100, 2) if abertura > 0 else 0.0
            vol = int(row["Volume"]) if pd.notnull(row["Volume"]) else 0
            
            registros.append({
                "data": d_dt,
                "abertura": abertura,
                "maxima": maxima,
                "minima": minima,
                "fechamento": fechamento,
                "ultimo": fechamento,
                "variacao": var,
                "volume": vol
            })
        df = pd.DataFrame(registros)
        df["data"] = pd.to_datetime(df["data"]).dt.date
        return df.drop_duplicates(subset=["data"], keep="last")
    except Exception as e:
        logger.warning(f"Não foi possível obter dados do IBOV: {e}")
        return pd.DataFrame()


def extrair_fechamento_dolar() -> pd.DataFrame:
    """Obtém cotações diárias do Dólar Comercial (USD/BRL) via AwesomeAPI."""
    try:
        logger.info("Extraindo cotações do Dólar USD/BRL via AwesomeAPI...")
        url = "https://economia.awesomeapi.com.br/json/daily/USD-BRL/15"
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            dados = resp.json()
            registros = []
            for item in dados:
                ts = int(item["timestamp"])
                d_dt = datetime.fromtimestamp(ts).date()
                compra = round(float(item["bid"]), 4)
                venda = round(float(item["ask"]), 4)
                maxima = round(float(item["high"]), 4)
                minima = round(float(item["low"]), 4)
                var = round(float(item["pctChange"]), 2)
                
                registros.append({
                    "data": d_dt,
                    "compra": compra,
                    "venda": venda,
                    "maxima": maxima,
                    "minima": minima,
                    "variacao": var
                })
            df = pd.DataFrame(registros)
            df["data"] = pd.to_datetime(df["data"]).dt.date
            return df.drop_duplicates(subset=["data"], keep="last")
        logger.warning(f"AwesomeAPI retornou status {resp.status_code}")
    except Exception as e:
        logger.warning(f"Não foi possível obter cotações do Dólar via AwesomeAPI: {e}")
    return pd.DataFrame()


# ==============================================================================
# 4. CARGA INCREMENTAL COM SQL MERGE NO BIGQUERY
# ==============================================================================
def upsert_fechamento_tickers(client: bigquery.Client, df: pd.DataFrame):
    """Executa o MERGE de cotações de tickers no BigQuery."""
    if df.empty:
        logger.info("Nenhum dado de ticker para atualizar.")
        return

    tabela_destino = f"{GCP_PROJECT_ID}.{DATASET_ID}.fechamento_tickers"
    tabela_temp = f"{GCP_PROJECT_ID}.{DATASET_ID}.tmp_fechamento_tickers"

    logger.info(f"Carregando {len(df)} linhas na tabela temporária '{tabela_temp}'...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, tabela_temp, job_config=job_config).result()

    query_merge = f"""
    MERGE `{tabela_destino}` T
    USING `{tabela_temp}` S
    ON T.ticker = S.ticker AND T.data = S.data
    WHEN MATCHED THEN
      UPDATE SET 
        preco = S.preco, 
        variacao = S.variacao, 
        dy = S.dy, 
        p_vp = S.p_vp, 
        volume = S.volume
    WHEN NOT MATCHED THEN
      INSERT (ticker, data, preco, variacao, dy, p_vp, volume)
      VALUES (S.ticker, S.data, S.preco, S.variacao, S.dy, S.p_vp, S.volume);
    """
    logger.info(f"Executando MERGE em '{tabela_destino}'...")
    client.query(query_merge).result()
    logger.info(f"✅ [SUCESSO] Tabela '{tabela_destino}' atualizada com sucesso ({len(df)} registros processados).")


def upsert_fechamento_ibov(client: bigquery.Client, df: pd.DataFrame):
    """Executa o MERGE do Ibovespa no BigQuery."""
    if df.empty:
        return

    tabela_destino = f"{GCP_PROJECT_ID}.{DATASET_ID}.fechamento_ibov"
    tabela_temp = f"{GCP_PROJECT_ID}.{DATASET_ID}.tmp_fechamento_ibov"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, tabela_temp, job_config=job_config).result()

    query_merge = f"""
    MERGE `{tabela_destino}` T
    USING `{tabela_temp}` S
    ON T.data = S.data
    WHEN MATCHED THEN
      UPDATE SET 
        abertura = S.abertura,
        maxima = S.maxima,
        minima = S.minima,
        fechamento = S.fechamento,
        ultimo = S.ultimo,
        variacao = S.variacao,
        volume = S.volume
    WHEN NOT MATCHED THEN
      INSERT (data, abertura, maxima, minima, fechamento, ultimo, variacao, volume)
      VALUES (S.data, S.abertura, S.maxima, S.minima, S.fechamento, S.ultimo, S.variacao, S.volume);
    """
    client.query(query_merge).result()
    logger.info(f"✅ [SUCESSO] Tabela '{tabela_destino}' atualizada.")


def upsert_fechamento_dolar(client: bigquery.Client, df: pd.DataFrame):
    """Executa o MERGE do Dólar no BigQuery."""
    if df.empty:
        return

    tabela_destino = f"{GCP_PROJECT_ID}.{DATASET_ID}.fechamento_dolar"
    tabela_temp = f"{GCP_PROJECT_ID}.{DATASET_ID}.tmp_fechamento_dolar"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, tabela_temp, job_config=job_config).result()

    query_merge = f"""
    MERGE `{tabela_destino}` T
    USING `{tabela_temp}` S
    ON T.data = S.data
    WHEN MATCHED THEN
      UPDATE SET 
        compra = S.compra,
        venda = S.venda,
        maxima = S.maxima,
        minima = S.minima,
        variacao = S.variacao
    WHEN NOT MATCHED THEN
      INSERT (data, compra, venda, maxima, minima, variacao)
      VALUES (S.data, S.compra, S.venda, S.maxima, S.minima, S.variacao);
    """
    client.query(query_merge).result()
    logger.info(f"✅ [SUCESSO] Tabela '{tabela_destino}' atualizada.")


# ==============================================================================
# 5. EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    logger.info("=" * 70)
    logger.info(f"INICIANDO ROTINA DE ATUALIZAÇÃO B3 -> BIGQUERY [{datetime.now()}]")
    logger.info("=" * 70)

    # 1. Conectar e validar estrutura no BigQuery
    client = obter_cliente_bigquery()
    garantir_dataset_e_tabelas(client)

    # 2. Extrair e fazer Upsert de Tickers da B3
    df_tickers = extrair_cotacoes_b3()
    upsert_fechamento_tickers(client, df_tickers)

    # 3. Extrair e fazer Upsert do Ibovespa
    df_ibov = extrair_fechamento_ibov()
    upsert_fechamento_ibov(client, df_ibov)

    # 4. Extrair e fazer Upsert do Dólar
    df_dolar = extrair_fechamento_dolar()
    upsert_fechamento_dolar(client, df_dolar)

    logger.info("=" * 70)
    logger.info("PIPELINE B3 FINALIZADO COM SUCESSO!")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
