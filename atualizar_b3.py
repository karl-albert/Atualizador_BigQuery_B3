#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ATUALIZADOR AUTOMÁTICO B3 - GOOGLE BIGQUERY (SANDBOX FREE TIER COMPATIBLE)
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

# Configuração de Logging
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
                sa_info = json.loads(GCP_SA_KEY)
                credentials = service_account.Credentials.from_service_account_info(sa_info)
                client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
                logger.info(f"Conectado ao BigQuery com Service Account no projeto '{GCP_PROJECT_ID}'.")
                return client
            except json.JSONDecodeError:
                if os.path.exists(GCP_SA_KEY):
                    credentials = service_account.Credentials.from_service_account_file(GCP_SA_KEY)
                    client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
                    logger.info(f"Conectado ao BigQuery via arquivo de credenciais no projeto '{GCP_PROJECT_ID}'.")
                    return client
        
        client = bigquery.Client(project=GCP_PROJECT_ID)
        logger.info(f"Conectado ao BigQuery via ADC no projeto '{GCP_PROJECT_ID}'.")
        return client
    except Exception as e:
        logger.error(f"Erro crítico ao inicializar cliente do BigQuery: {e}")
        raise e


def garantir_dataset(client: bigquery.Client):
    """Garante que o dataset B3 exista."""
    dataset_ref = f"{GCP_PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset '{dataset_ref}' validado.")
    except Exception:
        logger.info(f"Criando dataset '{dataset_ref}'...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset '{dataset_ref}' criado com sucesso.")


# ==============================================================================
# 2. EXTRAÇÃO DE COTAÇÕES DA B3
# ==============================================================================
def extrair_cotacoes_b3() -> pd.DataFrame:
    """Extrai cotações da B3 via API LUNN, Supabase ou Yahoo Finance."""
    # 1. Tentar API LUNN
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
        except Exception as e:
            logger.warning(f"Falha ao consultar API LUNN: {e}")

    # 2. Tentar Supabase intermediário
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

    # 3. Fallback: Yahoo Finance
    logger.info("Utilizando fallback automático via Yahoo Finance para mercado brasileiro (.SA)...")
    return extrair_b3_via_yfinance()


def normalizar_df_tickers(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza tipos e colunas da tabela de tickers."""
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

    df = df.dropna(subset=["ticker", "data"])
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
        "WEGE3.SA", "RENT3.SA", "SUZB3.SA", "GGBR4.SA", "RAIL3.SA", "EQTL3.SA",
        "LREN3.SA", "RADL3.SA", "PRIO3.SA", "RDOR3.SA", "VIVT3.SA", "CSNA3.SA",
        "CMIG4.SA", "SBSP3.SA", "TIMS3.SA", "UGPA3.SA", "HAPV3.SA", "ASAI3.SA",
        "EGIE3.SA", "KLBN11.SA", "MULT3.SA", "CYRE3.SA", "MRVE3.SA", "TOTS3.SA",
        "CVCB3.SA", "MGLU3.SA", "BHIA3.SA", "B3SA3.SA"
    ]
    
    logger.info(f"Baixando {len(tickers_b3)} ativos da B3 via Yahoo Finance (janela 5d)...")
    registros = []
    
    for t in tickers_b3:
        clean_ticker = t.replace(".SA", "")
        try:
            tk = yf.Ticker(t)
            hist = tk.history(period="5d", interval="1d")
            if hist.empty:
                continue
            for idx, row in hist.iterrows():
                d_dt = idx.date()
                preco = round(float(row["Close"]), 2)
                open_p = float(row["Open"]) if pd.notnull(row["Open"]) else preco
                var = round(((preco - open_p) / open_p) * 100, 2) if open_p > 0 else 0.0
                vol = int(row["Volume"]) if pd.notnull(row["Volume"]) else 0
                registros.append({
                    "ticker": clean_ticker,
                    "data": d_dt,
                    "preco": preco,
                    "variacao": var,
                    "dy": 0.0,
                    "p_vp": 0.0,
                    "volume": vol
                })
        except Exception as e:
            logger.debug(f"Aviso no ticker {t}: {e}")
            continue

    df = pd.DataFrame(registros)
    if df.empty:
        return pd.DataFrame()
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
        logger.warning(f"Não foi possível obter cotações do Dólar: {e}")
    return pd.DataFrame()


# ==============================================================================
# 4. CARGA INCREMENTAL (SANDBOX FREE TIER COMPATIBLE - SEM QUERIES DML MERGE)
# ==============================================================================
def upsert_tabela_sandbox(client: bigquery.Client, df_novos: pd.DataFrame, nome_tabela: str, chaves: list, schema: list = None):
    """
    Realiza carga/upsert incremental 100% compatível com BigQuery Free Tier (Sandbox).
    Substitui queries DML (MERGE/UPDATE) por Load Jobs com desduplicação em memória.
    """
    if df_novos is None or df_novos.empty:
        logger.info(f"Nenhum dado novo para a tabela '{nome_tabela}'.")
        return

    tabela_destino = f"{GCP_PROJECT_ID}.{DATASET_ID}.{nome_tabela}"
    
    # 1. Ler dados existentes se a tabela já existir
    try:
        query = f"SELECT * FROM `{tabela_destino}`"
        df_existente = client.query(query).to_dataframe()
        
        # Converter tipos de data para compatibilidade
        if "data" in df_existente.columns:
            df_existente["data"] = pd.to_datetime(df_existente["data"]).dt.date
            
        logger.info(f"Lidos {len(df_existente)} registros existentes de '{tabela_destino}'.")
        df_consolidado = pd.concat([df_existente, df_novos], ignore_index=True)
    except Exception as e:
        logger.info(f"Tabela '{tabela_destino}' ainda não possui registros ou é nova carga. Criando inicial...")
        df_consolidado = df_novos

    # 2. Desduplica mantendo sempre a versão mais recente
    df_consolidado = df_consolidado.drop_duplicates(subset=chaves, keep="last")
    
    # 3. Configurar Load Job com particionamento (WRITE_TRUNCATE substitui com base consolidada e limpa)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )
    if "data" in df_consolidado.columns:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data"
        )
    if "ticker" in df_consolidado.columns:
        job_config.clustering_fields = ["ticker"]
    if schema:
        job_config.schema = schema

    logger.info(f"Carregando {len(df_consolidado)} registros consolidados em '{tabela_destino}'...")
    client.load_table_from_dataframe(df_consolidado, tabela_destino, job_config=job_config).result()
    logger.info(f"✅ [SUCESSO] Tabela '{tabela_destino}' atualizada com sucesso ({len(df_consolidado)} registros totais).")


# ==============================================================================
# 5. EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    logger.info("=" * 70)
    logger.info(f"INICIANDO ROTINA DE ATUALIZAÇÃO B3 -> BIGQUERY [{datetime.now()}]")
    logger.info("=" * 70)

    # 1. Conectar e validar dataset
    client = obter_cliente_bigquery()
    garantir_dataset(client)

    # 2. Atualizar Tickers da B3
    df_tickers = extrair_cotacoes_b3()
    upsert_tabela_sandbox(client, df_tickers, "fechamento_tickers", chaves=["ticker", "data"])

    # 3. Atualizar Ibovespa
    df_ibov = extrair_fechamento_ibov()
    upsert_tabela_sandbox(client, df_ibov, "fechamento_ibov", chaves=["data"])

    # 4. Atualizar Dólar
    df_dolar = extrair_fechamento_dolar()
    upsert_tabela_sandbox(client, df_dolar, "fechamento_dolar", chaves=["data"])

    logger.info("=" * 70)
    logger.info("PIPELINE B3 FINALIZADO COM SUCESSO NO BIGQUERY!")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
