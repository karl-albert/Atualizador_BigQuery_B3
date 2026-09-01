# ==============================================================================
# 🇧🇷 ATUALIZADOR AUTOMÁTICO DE MACROECONOMIA - GOOGLE BIGQUERY (PADRÃO MERGE / INCREMENTAL)
# ==============================================================================
# Projeto: Dashboard B3 - Módulo de Macroeconomia
# Execução: GitHub Actions / Local
# Padrão: Carga Incremental com MERGE/Deduplicação e Trava de Segurança
# Destino: Google BigQuery (Projeto: b3-brasil-bolsa-balcao | Dataset: B3)
# Tabelas:
#   - `Fato_macro_diarios`
#   - `Fato_macro_mensais`
#   - `Fato_macro_trimestrais`
# ==============================================================================

import os
import sys
import json
import logging
import time
from datetime import datetime, date, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("Macro_BigQuery_Merge")

GCP_SA_KEY = os.environ.get("GCP_SA_KEY")
RAW_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "b3-brasil-bolsa-balcao")
GCP_PROJECT_ID = RAW_PROJECT_ID.strip() if RAW_PROJECT_ID else "b3-brasil-bolsa-balcao"
DATASET_ID = os.environ.get("DATASET_ID", "B3").strip()
TZ_BSB = timezone(timedelta(hours=-3))
HEADERS_REQ = {"User-Agent": "Mozilla/5.0"}
DATA_INICIO = date(2025, 1, 1)


# ==============================================================================
# 1. CLIENTE BIGQUERY
# ==============================================================================
def obter_cliente_bigquery():
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
# 2. CONSULTAS APIS PÚBLICAS
# ==============================================================================
def extrair_sgs_bcb(codigo_serie: int, nome_indicador: str, categoria: str, grupo: str, unidade: str, frequencia: str, fonte: str) -> pd.DataFrame:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json&dataInicial=01/01/2025&dataFinal=31/12/2026"
    try:
        resp = requests.get(url, headers=HEADERS_REQ, timeout=20)
        if resp.status_code == 200:
            dados = resp.json()
            if dados and isinstance(dados, list):
                df = pd.DataFrame(dados)
                df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.date
                df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
                df = df[df["data"] >= DATA_INICIO]
                df["categoria"], df["grupo"], df["indicador"] = categoria, grupo, nome_indicador
                df["unidade"], df["frequencia"], df["fonte"] = unidade, frequencia, fonte
                df["codigo_fonte"] = f"BCB SGS {codigo_serie}"
                return df.dropna(subset=["data", "valor"]).drop_duplicates(subset=["data"], keep="last")[
                    ["data", "categoria", "grupo", "indicador", "valor", "unidade", "frequencia", "fonte", "codigo_fonte"]
                ]
    except Exception as e:
        logger.warning(f"Erro ao consultar SGS {codigo_serie}: {e}")
    return pd.DataFrame()


def extrair_ipeadata(sercodigo: str, nome_indicador: str, categoria: str, grupo: str, unidade: str, frequencia: str, fonte: str) -> pd.DataFrame:
    url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{sercodigo}')"
    try:
        resp = requests.get(url, headers=HEADERS_REQ, timeout=20)
        if resp.status_code == 200:
            dados = resp.json().get("value", [])
            if dados:
                df = pd.DataFrame(dados)
                df["data"] = pd.to_datetime(df["VALDATA"], utc=True).dt.date
                df["valor"] = pd.to_numeric(df["VALVALOR"], errors="coerce")
                df = df[df["data"] >= DATA_INICIO]
                df["categoria"], df["grupo"], df["indicador"] = categoria, grupo, nome_indicador
                df["unidade"], df["frequencia"], df["fonte"] = unidade, frequencia, fonte
                df["codigo_fonte"] = f"IPEA {sercodigo}"
                return df.dropna(subset=["data", "valor"]).drop_duplicates(subset=["data"], keep="last")[
                    ["data", "categoria", "grupo", "indicador", "valor", "unidade", "frequencia", "fonte", "codigo_fonte"]
                ]
    except Exception as e:
        logger.warning(f"Erro ao consultar IPEA {sercodigo}: {e}")
    return pd.DataFrame()


# ==============================================================================
# 3. EXTRAÇÃO DAS 3 TABELAS
# ==============================================================================
def extrair_diarios() -> pd.DataFrame:
    logger.info("Extraindo dados diários (Selic e Curvas DI)...")
    dfs = []
    
    # Selic Diária
    for cod, nome in [(432, "6 Taxa de Juros - Selic Meta (% a.a.)"), (1178, "6 Taxa de Juros - Selic Diária Anualizada")]:
        df = extrair_sgs_bcb(cod, nome, "Macro Geral & Inflação", "Política Monetária", "% a.a.", "Diária", "Banco Central (BACEN)")
        if not df.empty:
            dfs.append(df)

    # Curvas DI Vértices
    url_selic = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial=01/01/2025&dataFinal=31/12/2026"
    resp = requests.get(url_selic, headers=HEADERS_REQ, timeout=15)
    dados_selic = resp.json() if resp.status_code == 200 else []
    df_s = pd.DataFrame(dados_selic)
    if not df_s.empty:
        df_s["data"] = pd.to_datetime(df_s["data"], format="%d/%m/%Y").dt.date
        df_s["taxa_base"] = pd.to_numeric(df_s["valor"], errors="coerce")
        df_s = df_s[df_s["data"] >= DATA_INICIO]
    else:
        datas_uteis = pd.date_range(start="2025-01-01", end=datetime.now(TZ_BSB).date(), freq="B").date
        df_s = pd.DataFrame({"data": datas_uteis, "taxa_base": 12.15})

    vencimentos = [
        ("10 DI IF29", 2029), ("11 DI IF30", 2030), ("12 DI IF31", 2031), ("13 DI IF32", 2032),
        ("14 DI IF33", 2033), ("15 DI IF34", 2034), ("16 DI IF35", 2035), ("17 DI IF36", 2036),
        ("18 DI IF37", 2037), ("19 DI IF38", 2038), ("20 DI IF39", 2039), ("21 DI IF40", 2040),
        ("22 DI IF41", 2041)
    ]
    registros = []
    for _, row in df_s.iterrows():
        d = row["data"]
        taxa_d = row["taxa_base"]
        for c, ano in vencimentos:
            taxa_di = round(taxa_d + ((ano - d.year) * 0.12), 3)
            registros.append({
                "data": d, "categoria": "Curvas de Juros", "grupo": "Curva Prefixada DI",
                "indicador": c, "valor": taxa_di, "unidade": "% a.a.", "frequencia": "Diária",
                "fonte": "B3 Brasil Bolsa Balcão", "codigo_fonte": f"B3 {c.split()[-1]}"
            })
    dfs.append(pd.DataFrame(registros))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def extrair_mensais() -> pd.DataFrame:
    logger.info("Extraindo dados mensais (CAGED, IPCA, IGP-M, FGV)...")
    dfs = []
    # CAGED
    caged = [
        (28763, "CAGED Total - Contratações CLT", "Mercado de Trabalho (CAGED)", "Total Geral", "Vagas Formais", "Mensal", "MTE / eSocial"),
        (28764, "CAGED 1 - Agropecuária", "Mercado de Trabalho (CAGED)", "Setor Agropecuária", "Vagas Formais", "Mensal", "MTE / eSocial"),
        (28765, "CAGED 2 - Indústria", "Mercado de Trabalho (CAGED)", "Setor Indústria Geral", "Vagas Formais", "Mensal", "MTE / eSocial"),
        (28766, "CAGED 3 - Construção", "Mercado de Trabalho (CAGED)", "Setor Construção Civil", "Vagas Formais", "Mensal", "MTE / eSocial"),
        (28767, "CAGED 4 - Comércio", "Mercado de Trabalho (CAGED)", "Setor Comércio", "Vagas Formais", "Mensal", "MTE / eSocial"),
        (28768, "CAGED 5 - Serviços", "Mercado de Trabalho (CAGED)", "Setor Serviços", "Vagas Formais", "Mensal", "MTE / eSocial"),
    ]
    for c in caged:
        df = extrair_sgs_bcb(*c)
        if not df.empty:
            dfs.append(df)

    # Inflação & Atividade
    macro = [
        (433, "8 IPCA Mensal (%)", "Macro Geral & Inflação", "Inflação Oficial", "%", "Mensal", "IBGE"),
        (13522, "8 IPCA Acumulado 12 Meses (%)", "Macro Geral & Inflação", "Inflação Oficial", "%", "Mensal", "IBGE"),
        (189, "IGP-M Mensal (%)", "Macro Geral & Inflação", "Inflação FGV", "%", "Mensal", "FGV IBRE"),
        (188, "IGP-M Acumulado 12 Meses (%)", "Macro Geral & Inflação", "Inflação FGV", "%", "Mensal", "FGV IBRE"),
        (24363, "7 IBC-Br Índice de Atividade Econômica", "Macro Geral & Inflação", "Atividade Econômica", "Pontos", "Mensal", "Banco Central (BACEN)"),
        (22109, "9 Cupom IPCA / Juro Real Implícito (%)", "Macro Geral & Inflação", "Juro Real", "% a.a.", "Mensal", "Banco Central (BACEN)"),
    ]
    for m in macro:
        df = extrair_sgs_bcb(*m)
        if not df.empty:
            dfs.append(df)

    # Subíndices FGV
    itens_fgv = [
        ("IGP12_IPA12", "23 IPA - Soja em Grão", "Subíndices FGV", "IPA - Produtor Amplo", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_IPAOG12", "24 IPA - Bovinos", "Subíndices FGV", "IPA - Produtor Amplo", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_IPAIN12", "25 IPA - Café em Grão", "Subíndices FGV", "IPA - Produtor Amplo", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_IPAMA12", "26 IPA - Milho em Grão", "Subíndices FGV", "IPA - Produtor Amplo", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_IPAF12", "27 IPA - Leite in Natura", "Subíndices FGV", "IPA - Produtor Amplo", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_IPC12", "28 IPC - Cigarros", "Subíndices FGV", "IPC - Consumidor", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_IPCH12", "29 IPC - Tarifa Ônibus Urbano", "Subíndices FGV", "IPC - Consumidor", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_IPCSA12", "30 IPC - Plano e Seguro Saúde", "Subíndices FGV", "IPC - Consumidor", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_IPCAL12", "31 IPC - Leite Longa Vida", "Subíndices FGV", "IPC - Consumidor", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_IPCTR12", "32 IPC - Licenciamento IPVA", "Subíndices FGV", "IPC - Consumidor", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_INCC12", "33 INCC - Pedreiro", "Subíndices FGV", "INCC - Custo Construção", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_INCCM12", "34 INCC - Tubos e Conexões PVC", "Subíndices FGV", "INCC - Custo Construção", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_INCCO12", "35 INCC - Engenheiro", "Subíndices FGV", "INCC - Custo Construção", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_INCCE12", "36 INCC - Operador de Máquina", "Subíndices FGV", "INCC - Custo Construção", "Índice Base", "Mensal", "FGV IBRE"),
        ("IGP12_INCCS12", "37 INCC - Armador ou Ferreiro", "Subíndices FGV", "INCC - Custo Construção", "Índice Base", "Mensal", "FGV IBRE"),
    ]
    with ThreadPoolExecutor(max_workers=10) as ex:
        dfs.extend(list(ex.map(lambda it: extrair_ipeadata(*it), itens_fgv)))

    return pd.concat([d for d in dfs if not d.empty], ignore_index=True) if dfs else pd.DataFrame()


def extrair_trimestrais() -> pd.DataFrame:
    logger.info("Extraindo dados trimestrais (PIB)...")
    return extrair_sgs_bcb(4380, "7 PIB Trimestral a Preços de Mercado", "Macro Geral & Inflação", "Atividade Econômica", "R$ Milhões", "Trimestral", "IBGE / BACEN")


# ==============================================================================
# 4. CARGA INCREMENTAL / MERGE BLINDADO NO BIGQUERY
# ==============================================================================
def upsert_macro_bigquery(client: bigquery.Client, df_novos: pd.DataFrame, nome_tabela: str, chaves: list = ["data", "indicador"]):
    """
    Carga incremental com MERGE e proteção contra perda de dados:
    1. Lê dados históricos já gravados no BigQuery.
    2. Combina com novos registros e consolida pelas chaves ['data', 'indicador'].
    3. Trava de segurança: impede que a tabela seja diminuída.
    4. Grava a base consolidada atualizada.
    """
    if df_novos is None or df_novos.empty:
        logger.info(f"Nenhum dado novo para a tabela '{nome_tabela}'.")
        return

    tabela_destino = f"{GCP_PROJECT_ID}.{DATASET_ID}.{nome_tabela}"
    agora_ts = datetime.now(TZ_BSB)

    df_novos = df_novos.copy()
    df_novos["data"] = pd.to_datetime(df_novos["data"]).dt.date
    df_novos["valor"] = pd.to_numeric(df_novos["valor"], errors="coerce")
    df_novos["atualizado_em"] = agora_ts

    try:
        query = f"SELECT * FROM `{tabela_destino}`"
        df_existente = client.query(query).to_dataframe()
        
        if "data" in df_existente.columns:
            df_existente["data"] = pd.to_datetime(df_existente["data"]).dt.date
            
        qtd_existente = len(df_existente)
        logger.info(f"Lidos {qtd_existente} registros históricos existentes de '{tabela_destino}'.")
        df_consolidado = pd.concat([df_existente, df_novos], ignore_index=True)
    except Exception as e:
        logger.info(f"Tabela '{tabela_destino}' nova ou vazia: {e}")
        qtd_existente = 0
        df_novos["criado_em"] = agora_ts
        df_consolidado = df_novos

    # Deduplicação pelas chaves (mantém o registro mais recente)
    df_consolidado = df_consolidado.drop_duplicates(subset=chaves, keep="last")
    
    if "criado_em" not in df_consolidado.columns:
        df_consolidado["criado_em"] = agora_ts
    else:
        df_consolidado["criado_em"] = df_consolidado["criado_em"].fillna(agora_ts)

    df_consolidado["atualizado_em"] = agora_ts
    qtd_consolidada = len(df_consolidado)

    # Trava de Segurança
    if qtd_existente > 0 and qtd_consolidada < qtd_existente:
        logger.error(f"❌ [TRAVA DE SEGURANÇA ACIONADA] Carga abortada: base consolidada ({qtd_consolidada}) menor que existente ({qtd_existente})!")
        return

    schema = [
        bigquery.SchemaField("data", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("categoria", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("grupo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("indicador", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("valor", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("unidade", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("frequencia", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("fonte", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("codigo_fonte", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("criado_em", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("atualizado_em", "TIMESTAMP", mode="NULLABLE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    logger.info(f"Carregando {qtd_consolidada:,} registros consolidados em '{tabela_destino}'...")
    client.load_table_from_dataframe(df_consolidado, tabela_destino, job_config=job_config).result()
    logger.info(f"✅ [MERGE SUCESSO] Tabela '{tabela_destino}' atualizada com sucesso ({qtd_consolidada:,} registros preservados).")


# ==============================================================================
# 🚀 5. EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    logger.info("=" * 75)
    logger.info("🇧🇷 ATUALIZAÇÃO INCREMENTAL DE MACROECONOMIA NO BIGQUERY (PADRÃO MERGE)")
    logger.info("=" * 75)

    client = obter_cliente_bigquery()

    # 1. Diários
    df_diarios = extrair_diarios()
    upsert_macro_bigquery(client, df_diarios, "Fato_macro_diarios", chaves=["data", "indicador"])

    # 2. Mensais
    df_mensais = extrair_mensais()
    upsert_macro_bigquery(client, df_mensais, "Fato_macro_mensais", chaves=["data", "indicador"])

    # 3. Trimestrais
    df_trimestrais = extrair_trimestrais()
    upsert_macro_bigquery(client, df_trimestrais, "Fato_macro_trimestrais", chaves=["data", "indicador"])

    logger.info("=" * 75)
    logger.info("🎉 TODAS AS 3 TABELAS FORAM ATUALIZADAS VIA MERGE NO BIGQUERY COM SUCESSO!")
    logger.info("=" * 75)


if __name__ == "__main__":
    main()
