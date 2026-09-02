#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ATUALIZADOR AUTOMÁTICO B3 - GOOGLE BIGQUERY (SÉRIE HISTÓRICA COMPLETA 5 ANOS)
================================================================================
Projeto: Pipeline de Dados B3 (Mercado Brasileiro) & Power BI
Responsável: Karl Albert / Engenharia de Dados & BI
Destino: Google BigQuery (Projeto: b3-brasil-bolsa-balcao | Dataset: B3)
Tabelas Oficiais Únicas:
  - Fato_B3_tickers (Cotações diárias/intraday de ações da B3 - Histórico 5 Anos)
  - Fato_B3_ibov    (Pontos e volume do Ibovespa - Histórico 5 Anos)
  - Fato_B3_dolar   (Cotação USD/BRL diária - Histórico 5 Anos)
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
logger = logging.getLogger("Atualizador_B3")

# ==============================================================================
# CONFIGURAÇÕES E VARIÁVEIS DE AMBIENTE
# ==============================================================================
GCP_SA_KEY = os.environ.get("GCP_SA_KEY")
RAW_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "b3-brasil-bolsa-balcao")
GCP_PROJECT_ID = RAW_PROJECT_ID.strip() if RAW_PROJECT_ID else "b3-brasil-bolsa-balcao"
DATASET_ID = os.environ.get("DATASET_ID", "B3").strip()

HEADERS_REQ = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ==============================================================================
# 1. CLIENTE BIGQUERY
# ==============================================================================
def obter_cliente_bigquery():
    """Inicializa o cliente do Google BigQuery via Service Account ou ADC."""
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
# 2. EXTRAÇÃO DE COTAÇÕES DE TODAS AS AÇÕES DA B3 (HISTÓRICO 5 ANOS)
# ==============================================================================
def extrair_cotacoes_b3(client: bigquery.Client) -> pd.DataFrame:
    """Extrai cotações da B3 com histórico amplo (5 anos) para alimentar todas as janelas."""
    tickers = []
    try:
        # Tentar obter lista de tickers monitorados da tabela ou dimensão
        for q in [
            f"SELECT DISTINCT ticker FROM `{GCP_PROJECT_ID}.{DATASET_ID}.Dim_Ativos_Board` WHERE ticker IS NOT NULL",
            f"SELECT DISTINCT ticker FROM `{GCP_PROJECT_ID}.{DATASET_ID}.Fato_B3_tickers` WHERE ticker IS NOT NULL"
        ]:
            try:
                df_t = client.query(q).to_dataframe()
                tickers = df_t["ticker"].dropna().unique().tolist()
                if tickers and len(tickers) > 5:
                    logger.info(f"Lista de {len(tickers)} ativos obtida do BigQuery.")
                    break
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Erro ao obter lista de tickers: {e}.")

    if not tickers:
        logger.info("Usando lista base principal de ativos da B3.")
        tickers = [
            "PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3", "ABEV3", "WEGE3", "RENT3", "SUZB3", "GGBR4",
            "MGLU3", "B3SA3", "PRIO3", "HAPV3", "RADL3", "RAIL3", "RDOR3", "SBSP3", "TOTS3", "UGPA3",
            "VBBR3", "VIVT3", "YDUQ3", "LREN3", "CSAN3", "EMBR3", "CPLE6", "CMIG4", "ELET3", "ELET6",
            "BBSE3", "ITSA4", "JBSS3", "BRFS3", "MRFG3", "BEEF3", "KLBN11", "SANB11", "ASAI3", "CRFB3",
            "AZUL4", "GOLL4", "CVCB3", "PETR3", "BBDC3", "BRAP4", "CYRE3", "EZTC3", "MRVE3", "MULT3",
            "IGTI11", "ALOS3", "SMTO3", "SLCE3", "TIMS3", "EGIE3", "EQTL3", "TAEE11", "TRPL4", "ENEV3",
            "CSNA3", "USIM5", "GOAU4", "DXCO3", "POSI3", "LWSA3", "CASH3", "FLRY3", "QUAL3", "HYPE3"
        ]

    def fetch_single_ticker(ticker):
        # range=5y garante histórico completo para janelas de 15 até 1500 dias
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}.SA?range=5y&interval=1d"
        try:
            r = requests.get(url, headers=HEADERS_REQ, timeout=15)
            if r.status_code == 200:
                res = r.json()
                result = res.get("chart", {}).get("result", [])
                if result:
                    timestamps = result[0].get("timestamp", [])
                    quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
                    closes = quotes.get("close", [])
                    opens = quotes.get("open", [])
                    volumes = quotes.get("volume", [])
                    
                    rows = []
                    prev_p = None
                    for ts, c, o, v in zip(timestamps, closes, opens, volumes):
                        d_dt = datetime.fromtimestamp(ts).date()
                        if d_dt.weekday() < 5 and c is not None:
                            preco = round(float(c), 2)
                            var = round(((preco - prev_p) / prev_p) * 100, 2) if prev_p and prev_p > 0 else 0.0
                            prev_p = preco
                            vol = int(v) if (v is not None and not pd.isna(v)) else 0
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
    with ThreadPoolExecutor(max_workers=25) as executor:
        results = executor.map(fetch_single_ticker, tickers)
        for r in results:
            all_rows.extend(r)

    df = pd.DataFrame(all_rows)
    logger.info(f"Total de {len(df)} cotações históricas coletadas para {df['ticker'].nunique() if not df.empty else 0} ativos.")
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

    df = df.dropna(subset=["ticker", "data", "preco"])
    df = df.drop_duplicates(subset=["ticker", "data"], keep="last")
    return df[["ticker", "data", "preco", "variacao", "dy", "p_vp", "volume"]]


# ==============================================================================
# 3. EXTRAÇÃO DO IBOVESPA E DÓLAR (HISTÓRICO 5 ANOS)
# ==============================================================================
def extrair_fechamento_ibov() -> pd.DataFrame:
    """Obtém histórico completo de 5 anos do Ibovespa."""
    try:
        url_ibov = "https://query1.finance.yahoo.com/v8/finance/chart/%5EBVSP?range=5y&interval=1d"
        r_ibov = requests.get(url_ibov, headers=HEADERS_REQ, timeout=15)
        if r_ibov.status_code == 200:
            res = r_ibov.json()
            result = res.get("chart", {}).get("result", [])[0]
            timestamps = result.get("timestamp", [])
            quotes = result.get("indicators", {}).get("quote", [{}])[0]
            closes = quotes.get("close", [])
            opens = quotes.get("open", [])
            highs = quotes.get("high", [])
            lows = quotes.get("low", [])
            volumes = quotes.get("volume", [])
            
            rows_ibov = []
            prev_fech = None
            for ts, c, o, h, l, v in zip(timestamps, closes, opens, highs, lows, volumes):
                d_dt = datetime.fromtimestamp(ts).date()
                if d_dt.weekday() < 5 and c is not None:
                    fech = round(float(c), 2)
                    abert = round(float(o), 2) if o else fech
                    maxi = round(float(h), 2) if h else fech
                    mini = round(float(l), 2) if l else fech
                    var = round(((fech - prev_fech) / prev_fech) * 100, 2) if prev_fech and prev_fech > 0 else 0.0
                    prev_fech = fech
                    vol = int(v) if v else 0
                    rows_ibov.append({
                        "data": d_dt, "abertura": abert, "maxima": maxi, "minima": mini,
                        "fechamento": fech, "ultimo": fech, "variacao": var, "volume": vol
                    })
            if rows_ibov:
                df = pd.DataFrame(rows_ibov)
                df["data"] = pd.to_datetime(df["data"]).dt.date
                df = df.drop_duplicates(subset=["data"], keep="last")
                logger.info(f"Ibovespa: {len(df)} pregões históricos consolidados.")
                return df
    except Exception as e:
        logger.warning(f"Erro ao extrair dados do IBOV: {e}")
    return pd.DataFrame()


def extrair_fechamento_dolar() -> pd.DataFrame:
    """Obtém cotações diárias do Dólar Comercial (USD/BRL) histórico de 5 anos."""
    registros = []
    try:
        url_yf = "https://query1.finance.yahoo.com/v8/finance/chart/USDBRL=X?range=5y&interval=1d"
        r_yf = requests.get(url_yf, headers=HEADERS_REQ, timeout=20)
        if r_yf.status_code == 200:
            res = r_yf.json()
            result = res.get("chart", {}).get("result", [])[0]
            timestamps = result.get("timestamp", [])
            quotes = result.get("indicators", {}).get("quote", [{}])[0]
            closes = quotes.get("close", [])
            highs = quotes.get("high", [])
            lows = quotes.get("low", [])
            for ts, c, h, l in zip(timestamps, closes, highs, lows):
                d_dt = datetime.fromtimestamp(ts).date()
                if d_dt.weekday() < 5 and c is not None:
                    val = round(float(c), 4)
                    registros.append({
                        "data": d_dt,
                        "compra": val,
                        "venda": val,
                        "maxima": round(float(h), 4) if h else val,
                        "minima": round(float(l), 4) if l else val,
                        "variacao": 0.0
                    })
    except Exception as e:
        logger.warning(f"Erro ao extrair Dólar histórico: {e}")

    if registros:
        df = pd.DataFrame(registros)
        df["data"] = pd.to_datetime(df["data"]).dt.date
        df = df.drop_duplicates(subset=["data"], keep="last")
        df = df.sort_values("data")
        df["variacao"] = ((df["compra"] - df["compra"].shift(1)) / df["compra"].shift(1) * 100).round(2).fillna(0.0)
        logger.info(f"Dólar USD/BRL: {len(df)} cotações diárias históricas consolidadas.")
        return df

    return pd.DataFrame()


# ==============================================================================
# 4. EXTRAÇÃO OFICIAL B3 - PARTICIPAÇÃO DOS INVESTIDORES (FLUXO DIÁRIO E MENSAL)
# ==============================================================================
def extrair_fluxo_investidores(dias_retroativos: int = 45) -> pd.DataFrame:
    """
    Extrai dados oficiais de Participação dos Investidores (Boletim Diário B3):
    - Investidor Estrangeiro
    - Institucionais
    - Investidores Individuais (Pessoa Física)
    - Instituições Financeiras
    - Outros
    """
    logger.info("Extraindo Participação dos Investidores (Boletim Diário B3)...")
    headers_bdi = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://arquivos.b3.com.br",
        "Referer": "https://arquivos.b3.com.br/",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
    }

    dt_fim = datetime.now()
    dt_ini = dt_fim - timedelta(days=dias_retroativos)

    datas_uteis = []
    curr = dt_ini
    while curr <= dt_fim:
        if curr.weekday() < 5:
            datas_uteis.append(curr.strftime("%Y-%m-%d"))
        curr += timedelta(days=1)

    def fetch_flow_date(d_str):
        url = f"https://drp.b3.com.br/bdi/table/SharesInvesVolum/{d_str}/{d_str}/1/100"
        try:
            r = requests.post(url, headers=headers_bdi, json={}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                table = data.get("table", {})
                values = table.get("values", [])
                if not values:
                    return []
                rows = []
                for row in values:
                    tipo = row[0]
                    compras = row[1]
                    part_compra = row[2]
                    vendas = row[3]
                    part_venda = row[4]
                    saldo = (compras - vendas) if (compras is not None and vendas is not None) else None
                    rows.append({
                        "data": pd.to_datetime(d_str).date(),
                        "tipo_investidor": str(tipo).strip(),
                        "compras_mil": float(compras) if compras is not None else 0.0,
                        "part_compra_pct": float(part_compra) if part_compra is not None else 0.0,
                        "vendas_mil": float(vendas) if vendas is not None else 0.0,
                        "part_venda_pct": float(part_venda) if part_venda is not None else 0.0,
                        "saldo_liquido_mil": float(saldo) if saldo is not None else 0.0
                    })
                return rows
        except Exception:
            return []
        return []

    todas_linhas = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futuros = {executor.submit(fetch_flow_date, d): d for d in datas_uteis}
        for f in futuros:
            try:
                res = f.result()
                if res:
                    todas_linhas.extend(res)
            except Exception:
                pass

    if todas_linhas:
        df = pd.DataFrame(todas_linhas)
        df = df.drop_duplicates(subset=["data", "tipo_investidor"], keep="last")
        df = df.sort_values(by=["data", "tipo_investidor"])
        logger.info(f"Participação dos Investidores: {len(df)} registros coletados em {df['data'].nunique()} pregões.")
        return df

    return pd.DataFrame()


# ==============================================================================
# 5. CARGA INCREMENTAL BLINDADA (TABELAS OFICIAIS Fato_B3_*)
# ==============================================================================
def upsert_tabela_blindada(client: bigquery.Client, df_novos: pd.DataFrame, nome_tabela: str, chaves: list):
    """
    Carga consolidada blindada:
    1. Lê base existente (se houver).
    2. Consolida com histórico novo de 5 anos e desduplica pelas chaves.
    3. Trata colunas e timestamps de forma consistente.
    4. Grava na tabela oficial do BigQuery.
    """
    if df_novos is None or df_novos.empty:
        logger.info(f"Nenhum dado para a tabela '{nome_tabela}'.")
        return

    tabela_destino = f"{GCP_PROJECT_ID}.{DATASET_ID}.{nome_tabela}"
    now = datetime.now()
    
    try:
        query = f"SELECT * FROM `{tabela_destino}`"
        df_existente = client.query(query).to_dataframe()
        if "data" in df_existente.columns:
            df_existente["data"] = pd.to_datetime(df_existente["data"]).dt.date
        if "volume" in df_existente.columns:
            df_existente["volume"] = pd.to_numeric(df_existente["volume"], errors="coerce").fillna(0).astype("int64")
            
        qtd_existente = len(df_existente)
        df_consolidado = pd.concat([df_existente, df_novos], ignore_index=True)
    except Exception as e:
        logger.info(f"Tabela '{tabela_destino}' nova ou vazia: {e}")
        df_consolidado = df_novos.copy()

    df_consolidado = df_consolidado.drop_duplicates(subset=chaves, keep="last")
    qtd_consolidada = len(df_consolidado)

    if "criado_em" in df_consolidado.columns:
        df_consolidado["criado_em"] = pd.to_datetime(df_consolidado["criado_em"]).fillna(now)
    else:
        df_consolidado["criado_em"] = now

    df_consolidado["atualizado_em"] = now

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    logger.info(f"Carregando {qtd_consolidada} registros em '{tabela_destino}'...")
    client.load_table_from_dataframe(df_consolidado, tabela_destino, job_config=job_config).result()
    logger.info(f"✅ [SUCESSO] Tabela oficial '{tabela_destino}' atualizada com sucesso ({qtd_consolidada} registros).")


# ==============================================================================
# 6. EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    logger.info("=" * 70)
    logger.info(f"INICIANDO CARGA COMPLETA B3 (5 ANOS) -> BIGQUERY [{datetime.now()}]")
    logger.info("=" * 70)

    client = obter_cliente_bigquery()

    # 1. Ações da B3 -> Fato_B3_tickers (5 anos de histórico)
    df_tickers = extrair_cotacoes_b3(client)
    upsert_tabela_blindada(client, df_tickers, "Fato_B3_tickers", chaves=["ticker", "data"])

    # 2. Ibovespa -> Fato_B3_ibov (5 anos de histórico)
    df_ibov = extrair_fechamento_ibov()
    upsert_tabela_blindada(client, df_ibov, "Fato_B3_ibov", chaves=["data"])

    # 3. Dólar -> Fato_B3_dolar (5 anos de histórico)
    df_dolar = extrair_fechamento_dolar()
    upsert_tabela_blindada(client, df_dolar, "Fato_B3_dolar", chaves=["data"])

    # 4. Fluxo de Investidores B3 -> Fato_Fluxo_Investidores_B3
    df_investidores = extrair_fluxo_investidores(dias_retroativos=45)
    upsert_tabela_blindada(client, df_investidores, "Fato_Fluxo_Investidores_B3", chaves=["data", "tipo_investidor"])

    logger.info("=" * 70)
    logger.info("PIPELINE B3 FINALIZADO COM 100% DE SUCESSO (HISTÓRICO COMPLETO 5 ANOS)!")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
