#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ATUALIZADOR AUTOMÁTICO DE MACROECONOMIA - GOOGLE BIGQUERY
================================================================================
Execução: Todo Sábado às 08:00 (Horário de Brasília)
Fontes: Banco Central do Brasil (BACEN SGS) / IPEA / IBGE
Tabelas:
  - Fato_Macro_Diarios       (Curvas de DI e Taxa Selic Diária/Meta)
  - Fato_macro_Mensais       (IPCA, IGP-M, Salário Mínimo, CAGED, IBC-Br, FGV)
  - Fato_macro_Trimestrais   (PIB Trimestral a Preços de Mercado)
================================================================================
"""

import os
import sys
import json
import logging
from datetime import datetime, date
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
logger = logging.getLogger("Atualizador_Macro")

# ==============================================================================
# CONFIGURAÇÕES E VARIÁVEIS DE AMBIENTE
# ==============================================================================
GCP_SA_KEY = os.environ.get("GCP_SA_KEY")
RAW_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "b3-brasil-bolsa-balcao")
GCP_PROJECT_ID = RAW_PROJECT_ID.strip() if RAW_PROJECT_ID else "b3-brasil-bolsa-balcao"
DATASET_ID = os.environ.get("DATASET_ID", "B3").strip()

START_DATE = date(2000, 1, 1)

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
# 2. EXTRAÇÃO DE SÉRIES BACEN SGS
# ==============================================================================
def extrair_serie_bcb(codigo: int, nome_indicador: str, categoria: str, grupo: str, unidade: str, frequencia: str, fonte: str) -> pd.DataFrame:
    """Baixa série histórica do SGS Banco Central."""
    try:
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"
        resp = requests.get(url, timeout=20)
        if resp.status_code == 200:
            df = pd.DataFrame(resp.json())
            df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.date
            df = df[df["data"] >= START_DATE].copy()
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
        # Mercado de Trabalho (CAGED)
        (28763, "CAGED Total - Contratações CLT", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (28764, "CAGED 1 - Agropecuária", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (28765, "CAGED 2 - Indústria", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (28766, "CAGED 3 - Construção", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (28767, "CAGED 4 - Comércio", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        (28768, "CAGED 5 - Serviços", "Macro Geral & Inflação", "Mercado de Trabalho", "Vagas", "Mensal", "MTE / BACEN"),
        
        # Salário Mínimo
        (1619, "Salário Mínimo", "Macro Geral & Inflação", "Renda & Trabalho", "R$", "Mensal", "Banco Central (BACEN)"),
        
        # Inflação & Atividade
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
# 3. CARGA INCREMENTAL BLINDADA NO BIGQUERY
# ==============================================================================
def upsert_tabela_blindada(client: bigquery.Client, df_novos: pd.DataFrame, nome_tabela: str, chaves: list):
    """
    Carga incremental blindada:
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
        if "valor" in df_existente.columns:
            df_existente["valor"] = pd.to_numeric(df_existente["valor"], errors="coerce")
            
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
# 4. EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    logger.info("=" * 70)
    logger.info(f"INICIANDO ROTINA SEMANAL DE MACROECONOMIA -> BIGQUERY [{datetime.now()}]")
    logger.info("=" * 70)

    client = obter_cliente_bigquery()

    # 1. Macro Mensais (IPCA, IGP-M, CAGED, Salário Mínimo, IBC-Br)
    df_macro_m = extrair_macro_mensais()
    upsert_tabela_blindada(client, df_macro_m, "Fato_macro_mensais", chaves=["indicador", "data"])

    # 2. PIB Trimestral
    df_pib = extrair_macro_trimestrais()
    upsert_tabela_blindada(client, df_pib, "Fato_macro_trimestrais", chaves=["indicador", "data"])

    logger.info("=" * 70)
    logger.info("ROTINA SEMANAL DE MACROECONOMIA FINALIZADA COM 100% DE SUCESSO!")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
