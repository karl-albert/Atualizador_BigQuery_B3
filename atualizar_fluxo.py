# -*- coding: utf-8 -*-
"""
Atualizador Histórico Completo: Fluxo de Investidores B3 -> BigQuery & CSV
Extrai 248 pregões históricos (1 ano) e carrega na tabela Fato_Fluxo_Investidores_B3
"""
import urllib.request
import json
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

print("=" * 70)
print("EXTRAINDO SERIE HISTORICA COMPLETA DE INVESTIDORES B3")
print("=" * 70)


url = "https://www.dadosdemercado.com.br/bolsa/investidores"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}
req = urllib.request.Request(url, headers=headers)
with urllib.request.urlopen(req) as resp:
    content = resp.read().decode("utf-8", errors="ignore")

idx = content.find("const data = [")
start = content.find("[", idx)
decoder = json.JSONDecoder()
api_data, _ = decoder.raw_decode(content[start:])

print(f"✅ Dados obtidos com sucesso: {len(api_data)} pregões!")

mapping = [
    ("foreigners", "Investidor Estrangeiro"),
    ("institutional", "Institucionais"),
    ("individuals", "Investidores Individuais"),
    ("financial_institutions", "Instituições Financeiras"),
    ("other", "Outros")
]

agora = datetime.now()
rows = []

for item in api_data:
    dt_str = item["date"]
    dt_val = datetime.strptime(dt_str, "%Y-%m-%d").date()
    for key, tipo in mapping:
        saldo = item.get(key)
        rows.append({
            "data": dt_val,
            "tipo_investidor": tipo,
            "compras_mil": None,
            "part_compra_pct": None,
            "vendas_mil": None,
            "part_venda_pct": None,
            "saldo_liquido_mil": float(saldo) if saldo is not None else None,
            "criado_em": agora,
            "atualizado_em": agora
        })

df_novo = pd.DataFrame(rows)
df_novo = df_novo.sort_values(by=["data", "tipo_investidor"]).reset_index(drop=True)
print(f"Linhas geradas: {len(df_novo)} registros ({df_novo['data'].nunique()} datas distintas)")
print(f"Período: {df_novo['data'].min()} até {df_novo['data'].max()}")

# 1. Salva CSV Local
csv_path = "Fato_Fluxo_Investidores_B3.csv"
df_novo.to_csv(csv_path, index=False, encoding="utf-8-sig")
print(f"✅ CSV local atualizado com sucesso: {csv_path}")

# 2. Conexão BigQuery
key_path = r"C:\Users\karla\Downloads\b3-brasil-bolsa-balcao-33d6ea23afa5.json"
creds = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(project="b3-brasil-bolsa-balcao", credentials=creds)

table_id = "b3-brasil-bolsa-balcao.B3.Fato_Fluxo_Investidores_B3"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("data", "DATE"),
        bigquery.SchemaField("tipo_investidor", "STRING"),
        bigquery.SchemaField("compras_mil", "FLOAT"),
        bigquery.SchemaField("part_compra_pct", "FLOAT"),
        bigquery.SchemaField("vendas_mil", "FLOAT"),
        bigquery.SchemaField("part_venda_pct", "FLOAT"),
        bigquery.SchemaField("saldo_liquido_mil", "FLOAT"),
        bigquery.SchemaField("criado_em", "DATETIME"),
        bigquery.SchemaField("atualizado_em", "DATETIME"),
    ],
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

print(f"\nCarregando {len(df_novo)} linhas para o BigQuery ({table_id})...")
job = client.load_table_from_dataframe(df_novo, table_id, job_config=job_config)
job.result()
print(f"🎉 Carga BigQuery concluída com sucesso! Tabela {table_id} possui agora {len(df_novo)} registros.")

# Validação final
query_val = f"SELECT MIN(data) as min_d, MAX(data) as max_d, COUNT(*) as cnt, COUNT(DISTINCT data) as cnt_d FROM `{table_id}`"
df_val = client.query(query_val).to_dataframe()
print("\n=== VALIDAÇÃO NO BIGQUERY ===")
print(df_val.to_string())
