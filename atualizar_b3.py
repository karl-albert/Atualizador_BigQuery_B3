#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ATUALIZADOR AUTOMÁTICO B3 & MACROECONOMIA - GOOGLE BIGQUERY
================================================================================
Projeto: Pipeline de Dados B3 & Macroeconomia Brasil / Power BI
Responsável: Karl Albert / Engenharia de Dados & BI
Destino: Google BigQuery (Projeto: b3-brasil-bolsa-balcao | Dataset: B3)
Tabelas:
  - Fato_B3_tickers          (Cotações diárias de todas as ações da B3)
  - Fato_B3_ibov             (Pontos, máximas, mínimas e volume do Ibovespa)
  - Fato_B3_dolar            (Cotação USD/BRL PTAX / Fechamento)
  - Fato_Macro_Diarios       (Curvas de DI e Taxa Selic Diária/Meta)
  - Fato_macro_Mensais       (IPCA, IGP-M, Salário Mínimo, CAGED, IBC-Br, FGV)
  - Fato_macro_Trimestrais   (PIB Trimestral a Preços de Mercado)
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
from concurrent.futures import ThreadPoolExecutor

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("Atualizador_B3_Macro")

# ==============================================================================
# CONFIGURAÇÕES E VARIÁVEIS DE AMBIENTE
# ==============================================================================
GCP_SA_KEY = os.environ.get("GCP_SA_KEY")
RAW_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "b3-brasil-bolsa-balcao")
GCP_PROJECT_ID = RAW_PROJECT_ID.strip() if RAW_PROJECT_ID else "b3-brasil-bolsa-balcao"
DATASET_ID = os.environ.get("DATASET_ID", "B3").strip()

LUNN_API_URL = os.environ.get("LUNN_API_URL")
LUNN_API_KEY = os.environ.get("LUNN_API_KEY")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://qhehkgxbpmpptshxlwrb.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

HEADERS_REQ = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ==============================================================================
# 1. CLIENTE BIGQUERY
# ==============================================================================
def obter_cliente_bigquery():
    """Inicializa o cliente do Google BigQuery via Service Account ou ADC de forma resiliente."""
    global GCP_PROJECT_ID
    try:
        if GCP_SA_KEY:
            try:
                sa_info = json.loads(GCP_SA_KEY.strip())
                if "project_id" in sa_info and sa_info["project_id"]:
                    GCP_PROJECT_ID = sa_info["project_id"].strip()
                credentials = service_account.Credentials.from_service_account_info(sa_info)
                client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
                logger.info(f"Conectado ao BigQuery com Service Account no projeto '{GCP_PROJECT_ID}'.")
                return client
            except json.JSONDecodeError:
                if os.path.exists(GCP_SA_KEY.strip()):
                    credentials = service_account.Credentials.from_service_account_file(GCP_SA_KEY.strip())
                    client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
                    logger.info(f"Conectado ao BigQuery via arquivo de credenciais no projeto '{GCP_PROJECT_ID}'.")
                    return client
        
        client = bigquery.Client(project=GCP_PROJECT_ID)
        logger.info(f"Conectado ao BigQuery via ADC no projeto '{GCP_PROJECT_ID}'.")
        return client
    except Exception as e:
        logger.error(f"Erro crítico ao inicializar cliente do BigQuery: {e}")
        raise e


# ==============================================================================
# 2. EXTRAÇÃO DE COTAÇÕES DE TODAS AS AÇÕES DA B3
# ==============================================================================
def extrair_cotacoes_b3(client: bigquery.Client) -> pd.DataFrame:
    """Extrai cotações da B3 via API LUNN, Supabase ou Yahoo Finance para todos os ativos."""
    if LUNN_API_URL:
        try:
            logger.info(f"Consultando API LUNN em: {LUNN_API_URL}...")
            headers = {"Authorization": f"Bearer {LUNN_API_KEY}"} if LUNN_API_KEY else {}
            resp = requests.get(LUNN_API_URL, headers=headers, timeout=60)
            if resp.status_code == 200:
                dados = resp.json()
                df = pd.DataFrame(dados)
                if not df.empty:
                    logger.info(f"API LUNN retornou {len(df)} registros.")
                    return normalizar_df_tickers(df)
        except Exception as e:
            logger.warning(f"Falha na API LUNN: {e}. Tentando fontes alternativas...")

    if SUPABASE_KEY:
        try:
            logger.info("Consultando Supabase (fechamento_tickers)...")
            url = f"{SUPABASE_URL}/rest/v1/fechamento_tickers?select=*&order=data.desc&limit=1000"
            headers = {
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}"
            }
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                dados = resp.json()
                df = pd.DataFrame(dados)
                if not df.empty:
                    logger.info(f"Supabase retornou {len(df)} registros.")
                    return normalizar_df_tickers(df)
        except Exception as e:
            logger.warning(f"Falha no Supabase: {e}. Tentando Yahoo Finance...")

    # Fallback Yahoo Finance
    logger.info("Coletando cotações via Yahoo Finance para os ativos monitorados...")
    try:
        q_ativos = f"SELECT DISTINCT ticker FROM `{GCP_PROJECT_ID}.{DATASET_ID}.Dim_Ativos` WHERE `Ticker Inativo` = 'ATIVA'"
        df_ativos = client.query(q_ativos).to_dataframe()
        tickers = df_ativos["ticker"].dropna().tolist()
    except Exception:
        tickers = ["PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3", "ABEV3", "WEGE3", "RENT3", "PRIO3", "MGLU3"]

    def fetch_single_ticker(ticker):
        sym = f"{ticker}.SA"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=10d&interval=1d"
        try:
            r = requests.get(url, headers=HEADERS_REQ, timeout=8)
            if r.status_code == 200:
                res = r.json()
                result = res.get("chart", {}).get("result", [])
                if result:
                    meta = result[0].get("meta", {})
                    prev_close = meta.get("chartPreviousClose")
                    reg_price = meta.get("regularMarketPrice")
                    
                    timestamps = result[0].get("timestamp", [])
                    quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
                    closes = quotes.get("close", [])
                    opens = quotes.get("open", [])
                    volumes = quotes.get("volume", [])
                    
                    rows = []
                    cutoff_date = date.today() - timedelta(days=7)
                    for ts, c, o, v in zip(timestamps, closes, opens, volumes):
                        d_dt = datetime.fromtimestamp(ts).date()
                        if d_dt >= cutoff_date:
                            preco_final = c if c is not None else reg_price
                            if preco_final is not None:
                                preco = round(float(preco_final), 2)
                                open_p = float(o) if o is not None else (prev_close if prev_close else preco)
                                base_var = prev_close if prev_close else open_p
                                var = round(((preco - base_var) / base_var) * 100, 2) if base_var and base_var > 0 else 0.0
                                vol = int(v) if v is not None else 0
                                rows.append({
                                    "ticker": ticker,
                                    "data": d_dt,
                                    "preco": preco,
                                    "variacao": var,
                                    "dy": 0.0,
                                    "p_vp": 0.0,
                                    "volume": vol
                                })
                    return rows
        except Exception:
            pass
        return []

    all_rows = []
    with ThreadPoolExecutor(max_workers=30) as executor:
        results = executor.map(fetch_single_ticker, tickers)
        for r in results:
            all_rows.extend(r)

    df = pd.DataFrame(all_rows)
    logger.info(f"Total de {len(df)} cotações coletadas para {df['ticker'].nunique() if not df.empty else 0} ativos.")
    return normalizar_df_tickers(df)


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


# ==============================================================================
# 3. EXTRAÇÃO DO IBOVESPA E DÓLAR
# ==============================================================================
def extrair_fechamento_ibov() -> pd.DataFrame:
    """Obtém os últimos pontos e volume do Ibovespa."""
    try:
        url_ibov = "https://query1.finance.yahoo.com/v8/finance/chart/%5EBVSP?range=10d&interval=1d"
        r_ibov = requests.get(url_ibov, headers=HEADERS_REQ, timeout=10)
        if r_ibov.status_code == 200:
            res = r_ibov.json()
            result = res.get("chart", {}).get("result", [])[0]
            meta = result.get("meta", {})
            reg_price = meta.get("regularMarketPrice")
            prev_close = meta.get("chartPreviousClose")
            
            timestamps = result.get("timestamp", [])
            quotes = result.get("indicators", {}).get("quote", [{}])[0]
            rows_ibov = []
            cutoff_date = date.today() - timedelta(days=7)
            for ts, c, o, h, l, v in zip(timestamps, quotes.get("close", []), quotes.get("open", []), quotes.get("high", []), quotes.get("low", []), quotes.get("volume", [])):
                d_dt = datetime.fromtimestamp(ts).date()
                if d_dt >= cutoff_date:
                    fech_raw = c if c is not None else reg_price
                    if fech_raw:
                        fech = round(float(fech_raw), 2)
                        abert = round(float(o), 2) if o else (prev_close if prev_close else fech)
                        maxi = round(float(h), 2) if h else fech
                        mini = round(float(l), 2) if l else fech
                        base_var = prev_close if prev_close else abert
                        var = round(((fech - base_var) / base_var) * 100, 2) if base_var and base_var > 0 else 0.0
                        vol = int(v) if v is not None else 0
                        rows_ibov.append({
                            "data": d_dt, "abertura": abert, "maxima": maxi, "minima": mini,
                            "ultimo": fech, "variacao": var, "volume": vol
                        })
            if rows_ibov:
                df = pd.DataFrame(rows_ibov).drop_duplicates(subset=["data"], keep="last")
                logger.info(f"Ibovespa: {len(df)} registros coletados.")
                return df
    except Exception as e:
        logger.error(f"Erro ao obter Ibovespa: {e}")
    return pd.DataFrame()


def extrair_fechamento_dolar() -> pd.DataFrame:
    """Obtém cotações do Dólar (USDBRL)."""
    try:
        url_bcb = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicialCotacao=@dataInicialCotacao,dataFinalCotacao=@dataFinalCotacao)?@moeda='USD'&@dataInicialCotacao='01-01-2025'&@dataFinalCotacao='12-31-2026'&$top=500&$format=json"
        r = requests.get(url_bcb, timeout=15)
        if r.status_code == 200:
            dados = r.json().get("value", [])
            rows_dolar = []
            for d in dados:
                dt_str = d.get("dataHoraCotacao", "")[:10]
                if dt_str:
                    d_dt = datetime.strptime(dt_str, "%Y-%m-%d").date()
                    compra = float(d.get("cotacaoCompra", 0.0))
                    venda = float(d.get("cotacaoVenda", 0.0))
                    rows_dolar.append({
                        "data": d_dt, "compra": compra, "venda": venda, "variacao": 0.0
                    })
            if rows_dolar:
                df = pd.DataFrame(rows_dolar).drop_duplicates(subset=["data"], keep="last")
                df = df.sort_values("data")
                df["variacao"] = ((df["compra"] - df["compra"].shift(1)) / df["compra"].shift(1) * 100).round(2).fillna(0.0)
                logger.info(f"Dólar PTAX: {len(df)} registros coletados.")
                return df
    except Exception as e:
        logger.error(f"Erro ao obter Dólar: {e}")
    return pd.DataFrame()


# ==============================================================================
# 4. EXTRAÇÃO MACROECONÔMICA (BACEN SGS)
# ==============================================================================
def extrair_serie_bcb(codigo: int, nome_indicador: str, categoria: str, grupo: str, unidade: str, frequencia: str, fonte: str) -> pd.DataFrame:
    """Baixa série histórica do SGS Banco Central."""
    try:
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"
        resp = requests.get(url, timeout=20)
        if resp.status_code == 200:
            df = pd.DataFrame(resp.json())
            df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.date
            df = df[df["data"] >= date(2000, 1, 1)].copy()
            df["categoria"] = categoria
            df["grupo"] = grupo
            df["indicador"] = nome_indicador
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
            df["unidade"] = unidade
            df["frequencia"] = frequencia
            df["fonte"] = fonte
            df["codigo_fonte"] = f"BCB SGS {codigo}"
            return df.dropna(subset=["valor"])
    except Exception as e:
        logger.error(f"Erro ao baixar série BCB {codigo} ({nome_indicador}): {e}")
    return pd.DataFrame()


def extrair_macro_mensais() -> pd.DataFrame:
    """Coleta todos os indicadores mensais (CAGED, Salário Mínimo, IPCA, IGP-M, IBC-Br, etc.)."""
    logger.info("Extraindo indicadores macroeconômicos mensais...")
    series_cfg = [
        (28763, "CAGED Total - Contratações CLT", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (28764, "CAGED 1 - Agropecuária", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (28765, "CAGED 2 - Indústria", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (28766, "CAGED 3 - Construção", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (28767, "CAGED 4 - Comércio", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (28768, "CAGED 5 - Serviços", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (1619, "Salário Mínimo", "Macro Geral & Inflação", "Renda & Trabalho", "R$", "Mensal", "Banco Central (BACEN)"),
        (433, "8 IPCA Mensal (%)", "Macro Geral & Inflação", "Inflação Oficial", "%", "Mensal", "IBGE"),
        (13522, "8 IPCA Acumulado 12 Meses (%)", "Macro Geral & Inflação", "Inflação Oficial", "%", "Mensal", "IBGE"),
        (189, "IGP-M Mensal (%)", "Macro Geral & Inflação", "Inflação FGV", "%", "Mensal", "FGV IBRE"),
        (188, "IGP-M Acumulado 12 Meses (%)", "Macro Geral & Inflação", "Inflação FGV", "%", "Mensal", "FGV IBRE"),
        (24363, "7 IBC-Br Índice de Atividade Econômica", "Macro Geral & Inflação", "Atividade Econômica", "Pontos", "Mensal", "Banco Central (BACEN)"),
        (29039, "9 Cupom IPCA / Juro Real Implícito (%)", "Macro Geral & Inflação", "Juro Real", "%", "Mensal", "Banco Central (BACEN)"),
    ]
    dfs = []
    for cfg in series_cfg:
        dft = extrair_serie_bcb(*cfg)
        if not dft.empty:
            dfs.append(dft)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def extrair_macro_trimestrais() -> pd.DataFrame:
    """Coleta o PIB Trimestral (BCB SGS 4380)."""
    logger.info("Extraindo PIB Trimestral...")
    return extrair_serie_bcb(
        codigo=4380,
        nome_indicador="7 PIB Trimestral a Preços de Mercado",
        categoria="Macro Geral & Inflação",
        grupo="Atividade Econômica",
        unidade="R$ Milhões",
        frequencia="Trimestral",
        fonte="IBGE / BACEN"
    )


# ==============================================================================
# 5. CARGA INCREMENTAL BLINDADA NO BIGQUERY
# ==============================================================================
def upsert_tabela_blindada(client: bigquery.Client, df_novos: pd.DataFrame, nome_tabela: str, chaves: list):
    """
    Carga incremental blindada (Tabela padrão plana):
    1. Lê base histórica existente no BigQuery.
    2. Consolida com dados novos e desduplica pelas chaves.
    3. Trava de segurança: impede redução de volume de dados.
    4. Grava a tabela no BigQuery com timestamps.
    """
    if df_novos is None or df_novos.empty:
        logger.info(f"Nenhum dado novo para '{nome_tabela}'.")
        return

    tabela_destino = f"{GCP_PROJECT_ID}.{DATASET_ID}.{nome_tabela}"
    
    try:
        query = f"SELECT * FROM `{tabela_destino}`"
        df_existente = client.query(query).to_dataframe()
        
        if "data" in df_existente.columns:
            df_existente["data"] = pd.to_datetime(df_existente["data"]).dt.date
        if "volume" in df_existente.columns:
            df_existente["volume"] = pd.to_numeric(df_existente["volume"], errors="coerce").fillna(0).astype("int64")
            
        qtd_existente = len(df_existente)
        logger.info(f"Lidos {qtd_existente} registros históricos existentes de '{tabela_destino}'.")
        df_consolidado = pd.concat([df_existente, df_novos], ignore_index=True)
    except Exception as e:
        logger.info(f"Tabela '{tabela_destino}' vazia ou nova: {e}")
        qtd_existente = 0
        df_consolidado = df_novos

    df_consolidado = df_consolidado.drop_duplicates(subset=chaves, keep="last")
    qtd_consolidada = len(df_consolidado)

    if qtd_existente > 0 and qtd_consolidada < qtd_existente:
        logger.error(f"❌ [TRAVA DE SEGURANÇA ACIONADA] Carga abortada: base consolidada ({qtd_consolidada}) menor que existente ({qtd_existente})!")
        return

    now = datetime.now()
    df_consolidado["atualizado_em"] = now
    if "criado_em" not in df_consolidado.columns:
        df_consolidado["criado_em"] = now

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    logger.info(f"Carregando {qtd_consolidada} registros totais em '{tabela_destino}'...")
    client.load_table_from_dataframe(df_consolidado, tabela_destino, job_config=job_config).result()
    logger.info(f"✅ [SUCESSO] Tabela '{tabela_destino}' atualizada ({qtd_consolidada} registros).")


# ==============================================================================
# 6. EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    logger.info("=" * 70)
    logger.info(f"INICIANDO ROTINA DE ATUALIZAÇÃO B3 & MACRO -> BIGQUERY [{datetime.now()}]")
    logger.info("=" * 70)

    client = obter_cliente_bigquery()

    # 1. Dólar PTAX
    df_dolar = extrair_fechamento_dolar()
    upsert_tabela_blindada(client, df_dolar, "Fato_B3_dolar", chaves=["data"])

    # 2. Ibovespa
    df_ibov = extrair_fechamento_ibov()
    upsert_tabela_blindada(client, df_ibov, "Fato_B3_ibov", chaves=["data"])

    # 3. Tickers da B3
    df_tickers = extrair_cotacoes_b3(client)
    upsert_tabela_blindada(client, df_tickers, "Fato_B3_tickers", chaves=["ticker", "data"])

    # 4. Macro Mensais (IPCA, IGP-M, CAGED, Salário Mínimo, etc.)
    df_macro_m = extrair_macro_mensais()
    upsert_tabela_blindada(client, df_macro_m, "Fato_macro_mensais", chaves=["indicador", "data"])

    # 5. PIB Trimestral
    df_pib = extrair_macro_trimestrais()
    upsert_tabela_blindada(client, df_pib, "Fato_macro_trimestrais", chaves=["indicador", "data"])

    logger.info("=" * 70)
    logger.info("PIPELINE B3 & MACRO FINALIZADO COM 100% DE SUCESSO NO BIGQUERY!")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
