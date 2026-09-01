#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
SCRIPT DE LIMPEZA DE TABELAS REDUNDANTES NO GOOGLE BIGQUERY
================================================================================
Projeto: Pipeline de Dados B3
Destino: Google BigQuery (Projeto: b3-brasil-bolsa-balcao | Dataset: B3)
Tabelas a serem excluídas:
  - Fato_B3_tickers
  - Fato_B3_ibov
  - Fato_B3_dolar
================================================================================
"""

import os
import sys
import json
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("Limpeza_B3")

GCP_SA_KEY = os.environ.get("GCP_SA_KEY")
RAW_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "b3-brasil-bolsa-balcao")
GCP_PROJECT_ID = RAW_PROJECT_ID.strip() if RAW_PROJECT_ID else "b3-brasil-bolsa-balcao"
DATASET_ID = os.environ.get("DATASET_ID", "B3").strip()

def obter_cliente_bigquery():
    global GCP_PROJECT_ID
    if GCP_SA_KEY:
        try:
            sa_info = json.loads(GCP_SA_KEY.strip())
            if "project_id" in sa_info and sa_info["project_id"]:
                GCP_PROJECT_ID = sa_info["project_id"].strip()
            credentials = service_account.Credentials.from_service_account_info(sa_info)
            client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
            logger.info(f"Conectado ao BigQuery com Service Account no projeto '{GCP_PROJECT_ID}'.")
            return client
        except Exception as e:
            logger.error(f"Erro ao decodificar GCP_SA_KEY: {e}")
    
    return bigquery.Client(project=GCP_PROJECT_ID)

def main():
    client = obter_cliente_bigquery()
    tabelas_para_apagar = [
        "Fato_B3_tickers",
        "Fato_B3_ibov",
        "Fato_B3_dolar"
    ]
    
    logger.info("Iniciando exclusão de tabelas redundantes...")
    for tab in tabelas_para_apagar:
        tabela_ref = f"{GCP_PROJECT_ID}.{DATASET_ID}.{tab}"
        try:
            client.delete_table(tabela_ref, not_found_ok=True)
            logger.info(f"✅ Tabela '{tabela_ref}' excluída com sucesso (ou já não existia).")
        except Exception as e:
            logger.warning(f"Não foi possível excluir '{tabela_ref}': {e}")
            
    logger.info("Processo de limpeza concluído com sucesso!")

if __name__ == "__main__":
    main()
