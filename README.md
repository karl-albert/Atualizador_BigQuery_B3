# 🇧🇷 Atualizador BigQuery B3 (Mercado Brasileiro)

Repositório automatizado para extração, tratamento e carga incremental de cotações da **B3 (Bolsa Brasileira)**, índice **Ibovespa** e **Dólar Comercial (USD/BRL)** diretamente no **Google BigQuery**, alimentando em tempo real o dashboard em **Power BI**.

---

## 🏗️ Arquitetura do Pipeline

```
[API LUNN / Fontes de Mercado] 
       │
       ▼
[GitHub Actions Runner (Python 3.11)]
       │
       ├─► Pandas (Normalização, Limpeza e Formatação de Tipos)
       ├─► Criação automática de tabelas particionadas no BigQuery
       │
       ▼
[SQL MERGE no Google BigQuery (Dataset: B3)]
  ├── `fechamento_tickers` (Particionada por Data e Clusterizada por Ticker)
  ├── `fechamento_ibov`    (Histórico de Pontos e Volume do Ibovespa)
  ├── `fechamento_dolar`   (Cotação de Compra, Venda e PTAX USD/BRL)
  └── `ativos_board`       (Tabela Mestra de Ativos e Setores)
       │
       ▼
[Power BI Desktop & Power BI Service (Refresh sem Gateway)]
```

---

## ⏱️ Grade Horária de Execução (GitHub Actions)

A automação está configurada no arquivo [`.github/workflows/rotina_b3.yml`](.github/workflows/rotina_b3.yml) com a seguinte grade (Horário de Brasília):

| Horário (Brasília) | Horário (UTC) | Objetivo |
| :--- | :--- | :--- |
| **10h30** | `13:30 UTC` | Carga de Abertura do Pregão B3 |
| **14h00** | `17:00 UTC` | Carga Intermediária |
| **18h30** | `21:30 UTC` | Fechamento Oficial do Pregão B3 |

> Além do agendamento automático, a rotina pode ser disparada manualmente a qualquer momento clicando em **Actions** $\rightarrow$ **Rotina Atualização B3 BigQuery** $\rightarrow$ **Run workflow**.

---

## 🔐 Configuração dos Segredos (GitHub Secrets)

No repositório do GitHub, vá em **Settings $\rightarrow$ Secrets and variables $\rightarrow$ Actions** e adicione:

| Secret | Descrição | Obrigatório? |
| :--- | :--- | :---: |
| **`GCP_SA_KEY`** | Conteúdo JSON completo da chave privada da Conta de Serviço do GCP. | **Sim** |
| **`GCP_PROJECT_ID`** | ID do projeto no Google Cloud (ex: `project-1c5de651-f9e1-439e-854`). | **Sim** |
| **`LUNN_API_URL`** | URL base do endpoint de fechamento da API LUNN. | Opcional |
| **`LUNN_API_KEY`** | Token Bearer de autenticação da API LUNN. | Opcional |
| **`SUPABASE_URL`** | URL da API do Supabase (para fallback). | Opcional |
| **`SUPABASE_KEY`** | Service Role / Anon Key do Supabase (para fallback). | Opcional |

---

## 💻 Como rodar localmente

```bash
# 1. Clonar o repositório
git clone https://github.com/karl-albert/Atualizador_BigQuery_B3.git
cd Atualizador_BigQuery_B3

# 2. Criar e ativar ambiente virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate    # Windows

# 3. Instalar dependências
pip install -r requirements.txt

# 4. Executar o script
python atualizar_b3.py
```
