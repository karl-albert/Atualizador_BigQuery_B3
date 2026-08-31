#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ATUALIZADOR AUTOMÁTICO B3 - GOOGLE BIGQUERY (PADRÃO NÃO-PARTICIONADO & ROBUSTO)
================================================================================
Projeto: Pipeline de Dados B3 (Mercado Brasileiro) & Power BI
Responsável: Karl Albert / Engenharia de Dados & BI
Destino: Google BigQuery (Projeto: b3-brasil-bolsa-balcao | Dataset: B3)
Tabelas (Padrão Não-Particionadas):
  - Fato_fechamento_tickers (Fato: Cotações diárias/intraday de todas as ações da B3)
  - Fato_fechamento_ibov    (Fato: Pontos, máximas, mínimas e volume do Ibovespa)
  - Fato_fechamento_dolar   (Fato: Cotação USD/BRL PTAX / Fechamento)
  - Din_ativos_board         (Dimensão: Ativos monitorados, setores e status)
================================================================================
"""

import os
import json
import time
import logging
from datetime import datetime, date, timedelta, timezone
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
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "b3-brasil-bolsa-balcao").strip()
DATASET_ID = os.environ.get("DATASET_ID", "B3").strip()

LUNN_API_URL = os.environ.get("LUNN_API_URL")
LUNN_API_KEY = os.environ.get("LUNN_API_KEY")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://qhehkgxbpmpptshxlwrb.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

HEADERS_REQ = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Configuração de Fuso Horário de Brasília
TZ_BSB = timezone(timedelta(hours=-3))


# ==============================================================================
# 1. CLIENTE BIGQUERY
# ==============================================================================
def obter_cliente_bigquery():
    """Inicializa o cliente do Google BigQuery via Service Account ou ADC de forma resiliente."""
    try:
        if GCP_SA_KEY:
            try:
                sa_info = json.loads(GCP_SA_KEY.strip())
                # IMPORTANTE: NÃO sobrescrever GCP_PROJECT_ID com o project_id
                # da chave JSON. A service account pode pertencer a um projeto
                # diferente do projeto de destino dos dados.
                sa_project = sa_info.get("project_id", "")
                if sa_project and sa_project.strip() != GCP_PROJECT_ID:
                    logger.warning(
                        f"SA pertence ao projeto '{sa_project.strip()}', "
                        f"mas o destino dos dados é '{GCP_PROJECT_ID}'. "
                        f"Usando '{GCP_PROJECT_ID}' como projeto de destino."
                    )
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
    """Extrai cotações da B3 via Yahoo Finance (com suporte a Intraday e Fechamento) ou LUNN/Supabase."""
    agora_bsb = datetime.now(TZ_BSB)
    eh_intraday = (agora_bsb.hour < 18) and (agora_bsb.weekday() < 5)
    logger.info(f"Modo de Extração: {'INTRADAY (Tempo Real)' if eh_intraday else 'FECHAMENTO CONSOLIDADO'} [{agora_bsb.strftime('%Y-%m-%d %H:%M:%S')}]")

    # Yahoo Finance como motor principal de alta performance em tempo real
    try:
        logger.info("Executando extração em lote para todos os tickers monitorados da B3...")
        df_yf = extrair_todos_tickers_yfinance(client)
        if df_yf is not None and not df_yf.empty:
            return df_yf
    except Exception as e:
        logger.warning(f"Falha na extração Yahoo Finance: {e}")

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

    if SUPABASE_URL and SUPABASE_KEY:
        try:
            logger.info(f"Buscando cotações no Supabase ({SUPABASE_URL})...")
            url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/fechamento_tickers?select=*&order=data.desc&limit=10000"
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

    return pd.DataFrame()


def extrair_todos_tickers_yfinance(client: bigquery.Client) -> pd.DataFrame:
    """Extrai cotações de todos os tickers da B3 com suporte a Intraday (Tempo Real) e Histórico."""
    try:
        q_tickers = f"SELECT DISTINCT ticker FROM `{GCP_PROJECT_ID}.{DATASET_ID}.Fato_fechamento_tickers` WHERE ticker IS NOT NULL"
        df_t = client.query(q_tickers).to_dataframe()
        tickers = df_t["ticker"].dropna().unique().tolist()
        logger.info(f"Lista de {len(tickers)} ativos obtida do BigQuery para atualização.")
    except Exception as e:
        logger.warning(f"Erro ao obter lista de tickers do BigQuery: {e}. Usando lista base.")
        tickers = ["PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3", "ABEV3", "WEGE3", "RENT3", "SUZB3", "GGBR4"]

    hoje_dt = datetime.now(TZ_BSB).date()
    cutoff_date = hoje_dt - timedelta(days=7)
    eh_dia_util = datetime.now(TZ_BSB).weekday() < 5

    def fetch_single_ticker(ticker):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}.SA?range=10d&interval=1d"
        try:
            r = requests.get(url, headers=HEADERS_REQ, timeout=10)
            if r.status_code == 200:
                res = r.json()
                result = res.get("chart", {}).get("result", [])
                if result:
                    meta = result[0].get("meta", {})
                    reg_price = meta.get("regularMarketPrice")
                    prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
                    reg_vol = meta.get("regularMarketVolume")

                    timestamps = result[0].get("timestamp", [])
                    quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
                    closes = quotes.get("close", [])
                    volumes = quotes.get("volume", [])

                    rows = []
                    # 1. Velas históricas anteriores a hoje
                    for i, (ts, c, v) in enumerate(zip(timestamps, closes, volumes)):
                        d_dt = datetime.fromtimestamp(ts, tz=TZ_BSB).date()
                        # Ignora fins de semana e data de hoje (hoje é processado via meta)
                        if d_dt >= cutoff_date and d_dt < hoje_dt and d_dt.weekday() < 5:
                            if c is not None:
                                preco = round(float(c), 2)
                                c_ant = closes[i-1] if i > 0 and closes[i-1] else (prev_close if prev_close else preco)
                                var = round(((preco - c_ant) / c_ant) * 100, 2) if c_ant and c_ant > 0 else 0.0
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

                    # 2. Posição Intraday / Fechamento de Hoje
                    if eh_dia_util and reg_price is not None:
                        preco_hoje = round(float(reg_price), 2)
                        base_d1 = prev_close if prev_close else (closes[-1] if closes and closes[-1] else preco_hoje)
                        var_hoje = round(((preco_hoje - base_d1) / base_d1) * 100, 2) if base_d1 and base_d1 > 0 else 0.0
                        vol_hoje = int(reg_vol) if reg_vol is not None else (int(volumes[-1]) if volumes and volumes[-1] else 0)
                        rows.append({
                            "ticker": ticker,
                            "data": hoje_dt,
                            "preco": preco_hoje,
                            "variacao": var_hoje,
                            "dy": 0.0,
                            "p_vp": 0.0,
                            "volume": vol_hoje
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

    # Remove finais de semana e nulos
    df = df.dropna(subset=["ticker", "data"])
    df = df[pd.to_datetime(df["data"]).dt.weekday < 5]
    df = df.drop_duplicates(subset=["ticker", "data"], keep="last")
    return df[["ticker", "data", "preco", "variacao", "dy", "p_vp", "volume"]]


# ==============================================================================
# 3. EXTRAÇÃO DO IBOVESPA E DÓLAR
# ==============================================================================
def extrair_fechamento_ibov() -> pd.DataFrame:
    """Obtém os últimos pontos e volume do Ibovespa (Intraday e Histórico)."""
    try:
        url_ibov = "https://query1.finance.yahoo.com/v8/finance/chart/%5EBVSP?range=10d&interval=1d"
        r_ibov = requests.get(url_ibov, headers=HEADERS_REQ, timeout=10)
        if r_ibov.status_code == 200:
            res = r_ibov.json()
            result = res.get("chart", {}).get("result", [])[0]
            meta = result.get("meta", {})
            reg_price = meta.get("regularMarketPrice")
            prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
            reg_vol = meta.get("regularMarketVolume", 0)

            timestamps = result.get("timestamp", [])
            quotes = result.get("indicators", {}).get("quote", [{}])[0]
            closes = quotes.get("close", [])
            opens = quotes.get("open", [])
            highs = quotes.get("high", [])
            lows = quotes.get("low", [])
            volumes = quotes.get("volume", [])

            hoje_dt = datetime.now(TZ_BSB).date()
            cutoff_date = hoje_dt - timedelta(days=10)
            rows_ibov = []

            # Histórico
            for i, (ts, c, o, h, l, v) in enumerate(zip(timestamps, closes, opens, highs, lows, volumes)):
                d_dt = datetime.fromtimestamp(ts, tz=TZ_BSB).date()
                if d_dt >= cutoff_date and d_dt < hoje_dt and d_dt.weekday() < 5:
                    if c is not None:
                        fech = round(float(c), 2)
                        abert = round(float(o), 2) if o else fech
                        maxi = round(float(h), 2) if h else fech
                        mini = round(float(l), 2) if l else fech
                        c_ant = closes[i-1] if i > 0 and closes[i-1] else (prev_close if prev_close else abert)
                        var = round(((fech - c_ant) / c_ant) * 100, 2) if c_ant else 0.0
                        vol = int(v) if v else 0
                        rows_ibov.append({
                            "data": d_dt, "abertura": abert, "maxima": maxi, "minima": mini,
                            "fechamento": fech, "ultimo": fech, "variacao": var, "volume": vol
                        })

            # Intraday Hoje
            if datetime.now(TZ_BSB).weekday() < 5 and reg_price is not None:
                fech_hoje = round(float(reg_price), 2)
                base_ibov = prev_close if prev_close else (closes[-1] if closes and closes[-1] else fech_hoje)
                var_hoje = round(((fech_hoje - base_ibov) / base_ibov) * 100, 2) if base_ibov else 0.0
                rows_ibov.append({
                    "data": hoje_dt, "abertura": fech_hoje, "maxima": fech_hoje, "minima": fech_hoje,
                    "fechamento": fech_hoje, "ultimo": fech_hoje, "variacao": var_hoje,
                    "volume": int(reg_vol) if reg_vol else 0
                })

            if rows_ibov:
                df = pd.DataFrame(rows_ibov)
                df["data"] = pd.to_datetime(df["data"]).dt.date
                df = df[pd.to_datetime(df["data"]).dt.weekday < 5]
                return df.drop_duplicates(subset=["data"], keep="last")
    except Exception as e:
        logger.warning(f"Não foi possível obter dados do IBOV: {e}")
    return pd.DataFrame()


def extrair_fechamento_dolar(max_tentativas: int = 3) -> pd.DataFrame:
    """Obtém cotações do Dólar Comercial (USD/BRL) via AwesomeAPI (Histórico e Intraday com retry)."""
    registros = []
    hoje_dt = datetime.now(TZ_BSB).date()

    # 1. Histórico dos últimos 15 dias úteis com retry
    url_hist = "https://economia.awesomeapi.com.br/json/daily/USD-BRL/15"
    for tentativa in range(1, max_tentativas + 1):
        try:
            logger.info(f"[DOLAR] Chamando AwesomeAPI Histórico (tentativa {tentativa}/{max_tentativas})...")
            resp = requests.get(url_hist, headers=HEADERS_REQ, timeout=20)
            if resp.status_code == 200:
                dados = resp.json()
                logger.info(f"[DOLAR] API histórico retornou {len(dados)} registros.")
                for item in dados:
                    ts = int(item["timestamp"])
                    d_dt = datetime.fromtimestamp(ts, tz=TZ_BSB).date()
                    if d_dt.weekday() < 5:
                        registros.append({
                            "data": d_dt,
                            "compra": round(float(item["bid"]), 4),
                            "venda": round(float(item["ask"]), 4),
                            "maxima": round(float(item["high"]), 4),
                            "minima": round(float(item["low"]), 4),
                            "variacao": round(float(item["pctChange"]), 2)
                        })
                break
            elif resp.status_code == 429:
                espera = 15 * tentativa
                logger.warning(f"[DOLAR] Rate limit (429). Aguardando {espera}s...")
                time.sleep(espera)
            else:
                logger.error(f"[DOLAR] Status {resp.status_code}: {resp.text[:200]}")
                break
        except Exception as e:
            logger.warning(f"[DOLAR] Erro na tentativa {tentativa}: {e}")
            if tentativa < max_tentativas:
                time.sleep(5)

    # 2. Intraday em tempo real
    try:
        url_last = "https://economia.awesomeapi.com.br/last/USD-BRL"
        resp_last = requests.get(url_last, headers=HEADERS_REQ, timeout=15)
        if resp_last.status_code == 200:
            item = resp_last.json().get("USDBRL", {})
            if item and datetime.now(TZ_BSB).weekday() < 5:
                registros.append({
                    "data": hoje_dt,
                    "compra": round(float(item["bid"]), 4),
                    "venda": round(float(item["ask"]), 4),
                    "maxima": round(float(item["high"]), 4),
                    "minima": round(float(item["low"]), 4),
                    "variacao": round(float(item["pctChange"]), 2)
                })
    except Exception as e:
        logger.warning(f"Não foi possível obter cotação live do Dólar: {e}")

    if registros:
        df = pd.DataFrame(registros)
        df["data"] = pd.to_datetime(df["data"]).dt.date
        df = df[pd.to_datetime(df["data"]).dt.weekday < 5]
        return df.drop_duplicates(subset=["data"], keep="last")
    return pd.DataFrame()


# ==============================================================================
# 4. CARGA INCREMENTAL BLINDADA (TABELA PADRÃO NÃO-PARTICIONADA)
# ==============================================================================
def upsert_tabela_blindada(client: bigquery.Client, df_novos: pd.DataFrame, nome_tabela: str, chaves: list):
    """
    Carga incremental blindada (Tabela padrão não-particionada):
    1. Lê a base completa existente no BigQuery.
    2. Consolida com os novos registros e desduplica pelas chaves.
    3. Trava de segurança: impede redução de volume de dados.
    4. Grava em tabela padrão plana.
    """
    if df_novos is None or df_novos.empty:
        logger.info(f"Nenhum dado novo para a tabela '{nome_tabela}'.")
        return

    # Garante expurgo de fins de semana (sábado e domingo)
    if "data" in df_novos.columns:
        df_novos = df_novos[pd.to_datetime(df_novos["data"]).dt.weekday < 5]

    tabela_destino = f"{GCP_PROJECT_ID}.{DATASET_ID}.{nome_tabela}"
    logger.info(f"[UPSERT] Destino: {tabela_destino}")
    logger.info(f"[UPSERT] Novos registros: {len(df_novos)} | Colunas: {list(df_novos.columns)}")
    if "data" in df_novos.columns:
        logger.info(f"[UPSERT] Datas nos novos: {sorted(df_novos['data'].unique())[-5:]}")

    try:
        query = f"SELECT * FROM `{tabela_destino}`"
        df_existente = client.query(query).to_dataframe()

        if "data" in df_existente.columns:
            df_existente["data"] = pd.to_datetime(df_existente["data"]).dt.date
            # Limpa fins de semana legados da base histórica se existirem
            df_existente = df_existente[pd.to_datetime(df_existente["data"]).dt.weekday < 5]
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

    if qtd_existente > 0 and qtd_consolidada < (qtd_existente - 50):
        logger.error(f"❌ [TRAVA DE SEGURANÇA ACIONADA] Carga abortada: base consolidada ({qtd_consolidada}) muito menor que existente ({qtd_existente})!")
        return

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    logger.info(f"Carregando {qtd_consolidada} registros totais em '{tabela_destino}' (Padrão Não-Particionada)...")
    client.load_table_from_dataframe(df_consolidado, tabela_destino, job_config=job_config).result()
    logger.info(f"✅ [SUCESSO] Tabela '{tabela_destino}' atualizada com sucesso ({qtd_consolidada} registros preservados).")


# ==============================================================================
# 5. EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    logger.info("=" * 70)
    logger.info(f"INICIANDO ROTINA DE ATUALIZAÇÃO B3 -> BIGQUERY [{datetime.now(TZ_BSB)}]")
    logger.info("=" * 70)

    client = obter_cliente_bigquery()

    # 1. Atualizar Dólar (primeiro, para garantir cotação cambial do dia)
    df_dolar = extrair_fechamento_dolar()
    upsert_tabela_blindada(client, df_dolar, "Fato_fechamento_dolar", chaves=["data"])

    # 2. Atualizar Ibovespa
    df_ibov = extrair_fechamento_ibov()
    upsert_tabela_blindada(client, df_ibov, "Fato_fechamento_ibov", chaves=["data"])

    # 3. Atualizar Tickers da B3
    df_tickers = extrair_cotacoes_b3(client)
    upsert_tabela_blindada(client, df_tickers, "Fato_fechamento_tickers", chaves=["ticker", "data"])

    logger.info("=" * 70)
    logger.info("PIPELINE B3 FINALIZADO COM SUCESSO NO BIGQUERY!")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
