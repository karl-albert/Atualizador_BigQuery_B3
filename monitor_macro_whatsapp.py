#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
MONITOR DE INDICADORES MACROECONÔMICOS COM ALERTA NO WHATSAPP
================================================================================
Monitora publicações de novos dados oficiais (Salário Mínimo, IPCA, IGP-M, PIB, CAGED)
e dispara notificações instantâneas no WhatsApp via CallMeBot ou Webhook Customizado.
================================================================================
"""

import os
import sys
import json
import logging
from datetime import datetime, date
import urllib.parse
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("Monitor_WhatsApp")

# ==============================================================================
# CONFIGURAÇÕES E CREDENCIAIS
# ==============================================================================
GCP_SA_KEY = os.environ.get("GCP_SA_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "b3-brasil-bolsa-balcao").strip()
DATASET_ID = os.environ.get("DATASET_ID", "B3").strip()

# Configurações do WhatsApp
WHATSAPP_PHONE = os.environ.get("WHATSAPP_PHONE")        # Ex: "5511999999999" (com DDI 55 + DDD)
CALLMEBOT_API_KEY = os.environ.get("CALLMEBOT_API_KEY")  # Chave obtida no CallMeBot
WHATSAPP_WEBHOOK_URL = os.environ.get("WHATSAPP_WEBHOOK_URL") # URL de Webhook alternativa (n8n/Evolution/Z-API)

# ==============================================================================
# 1. CLIENTE BIGQUERY
# ==============================================================================
def obter_cliente_bigquery():
    try:
        if GCP_SA_KEY:
            try:
                sa_info = json.loads(GCP_SA_KEY.strip())
                credentials = service_account.Credentials.from_service_account_info(sa_info)
                return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
            except json.JSONDecodeError:
                if os.path.exists(GCP_SA_KEY.strip()):
                    credentials = service_account.Credentials.from_service_account_file(GCP_SA_KEY.strip())
                    return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
        
        # Local fallback para chave padrão se existir
        local_key = r"C:\Users\karla\Downloads\b3-brasil-bolsa-balcao-33d6ea23afa5.json"
        if os.path.exists(local_key):
            credentials = service_account.Credentials.from_service_account_file(local_key)
            return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

        return bigquery.Client(project=GCP_PROJECT_ID)
    except Exception as e:
        logger.error(f"Erro ao conectar ao BigQuery: {e}")
        return None


# ==============================================================================
# 2. ENVIO DE MENSAGEM NO WHATSAPP
# ==============================================================================
def enviar_alerta_whatsapp(titulo: str, indicador: str, valor_novo: float, valor_ant: float, data_ref: str, unidade: str):
    """Envia alerta formatado para o WhatsApp."""
    var_txt = ""
    if valor_ant and valor_ant > 0:
        pct = ((valor_novo - valor_ant) / valor_ant) * 100
        seta = "🔺" if pct > 0 else ("🔻" if pct < 0 else "🔹")
        var_txt = f"\n*Variação:* {seta} {pct:+.2f}%"

    msg = (
        f"📊 *ALERTA MACRO B3 - NOVO DADO PUBLICADO*\n\n"
        f"📌 *Indicador:* {indicador}\n"
        f"💰 *Novo Valor:* {unidade} {valor_novo:,.2f}\n"
        f"📅 *Data de Vigência:* {data_ref}"
        f"{var_txt}\n\n"
        f"⚡ _Google BigQuery e Dashboard Power BI sincronizados com sucesso._"
    )

    logger.info(f"Notificação gerada para '{indicador}':\n{msg}")

    # Método 1: CallMeBot WhatsApp API
    if WHATSAPP_PHONE and CALLMEBOT_API_KEY:
        try:
            phone_clean = WHATSAPP_PHONE.replace("+", "").replace("-", "").replace(" ", "").strip()
            msg_encoded = urllib.parse.quote(msg)
            url = f"https://api.callmebot.com/whatsapp.php?phone={phone_clean}&text={msg_encoded}&apikey={CALLMEBOT_API_KEY}"
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                logger.info("✅ [WHATSAPP] Mensagem enviada com sucesso via CallMeBot!")
                return True
            else:
                logger.warning(f"Aviso CallMeBot HTTP {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"Erro ao enviar via CallMeBot: {e}")

    # Método 2: Webhook Customizado (n8n / Evolution API / Z-API)
    if WHATSAPP_WEBHOOK_URL:
        try:
            payload = {
                "phone": WHATSAPP_PHONE,
                "message": msg,
                "indicador": indicador,
                "valor_novo": valor_novo,
                "valor_anterior": valor_ant,
                "data_referencia": data_ref,
                "unidade": unidade
            }
            resp = requests.post(WHATSAPP_WEBHOOK_URL, json=payload, timeout=15)
            if resp.status_code in [200, 201, 204]:
                logger.info("✅ [WHATSAPP] Webhook customizado disparado com sucesso!")
                return True
        except Exception as e:
            logger.error(f"Erro ao disparar Webhook customizado: {e}")

    return False


# ==============================================================================
# 3. VERIFICAÇÃO DE MUDANÇA EM SÉRIES OFICIAIS
# ==============================================================================
def verificar_indicador(client: bigquery.Client, codigo_bcb: int, nome_indicador: str, tabela_bq: str, unidade: str):
    """Verifica se há medição mais recente no BACEN em relação ao BigQuery."""
    try:
        # 1. Pega último valor da API do Banco Central
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_bcb}/dados/ultimos/2?formato=json"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return

        dados_api = resp.json()
        if not dados_api:
            return

        ultimo_api = dados_api[-1]
        data_api_str = ultimo_api["data"] # "dd/mm/yyyy"
        data_api_iso = datetime.strptime(data_api_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        valor_api = float(ultimo_api["valor"])
        valor_ant_api = float(dados_api[0]["valor"]) if len(dados_api) > 1 else None

        # 2. Pega último valor registrado no BigQuery
        q = f"""
            SELECT data, valor 
            FROM `{GCP_PROJECT_ID}.{DATASET_ID}.{tabela_bq}`
            WHERE indicador = '{nome_indicador}'
            ORDER BY data DESC
            LIMIT 1
        """
        df_bq = client.query(q).to_dataframe()
        
        if df_bq.empty:
            logger.info(f"Nenhum dado prévio no BigQuery para '{nome_indicador}'.")
            return

        data_bq_iso = str(df_bq.iloc[0]["data"])
        valor_bq = float(df_bq.iloc[0]["valor"])

        # 3. Compara se a data ou o valor mudou
        if data_api_iso > data_bq_iso or (data_api_iso == data_bq_iso and valor_api != valor_bq):
            logger.info(f"🔥 NOVO DADO DETECTADO para '{nome_indicador}'! API: {valor_api} ({data_api_iso}) vs BQ: {valor_bq} ({data_bq_iso})")
            
            # Dispara Alerta no WhatsApp
            enviar_alerta_whatsapp(
                titulo="Novo Dado Oficial Publicado",
                indicador=nome_indicador,
                valor_novo=valor_api,
                valor_ant=valor_bq,
                data_ref=data_api_str,
                unidade=unidade
            )
        else:
            logger.info(f"Sem novidades para '{nome_indicador}' (Último oficial: {data_bq_iso} = {valor_bq}).")

    except Exception as e:
        logger.error(f"Erro ao verificar indicador '{nome_indicador}': {e}")


# ==============================================================================
# 4. EXECUÇÃO DO MONITOR
# ==============================================================================
def main():
    logger.info("=" * 70)
    logger.info(f"INICIANDO MONITOR DE INDICADORES MACROECONÔMICOS [{datetime.now()}]")
    logger.info("=" * 70)

    client = obter_cliente_bigquery()
    if not client:
        logger.error("Falha de conexão com o BigQuery. Abortando monitoramento.")
        return

    # Indicadores Monitorados
    monitores = [
        {"codigo": 1619, "nome": "Salário Mínimo", "tabela": "Fato_macro_mensais", "unid": "R$"},
        {"codigo": 433, "nome": "8 IPCA Mensal (%)", "tabela": "Fato_macro_mensais", "unid": "%"},
        {"codigo": 189, "nome": "IGP-M Mensal (%)", "tabela": "Fato_macro_mensais", "unid": "%"},
        {"codigo": 4380, "nome": "7 PIB Trimestral a Preços de Mercado", "tabela": "Fato_macro_trimestrais", "unid": "R$ Mi"},
        {"codigo": 28763, "nome": "CAGED Total - Contratações CLT", "tabela": "Fato_macro_mensais", "unid": "Vagas"}
    ]

    for m in monitores:
        verificar_indicador(
            client=client,
            codigo_bcb=m["codigo"],
            nome_indicador=m["nome"],
            tabela_bq=m["tabela"],
            unidade=m["unid"]
        )

    logger.info("=" * 70)
    logger.info("MONITORAMENTO CONCLUÍDO.")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
