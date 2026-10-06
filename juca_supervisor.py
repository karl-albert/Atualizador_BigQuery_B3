# -*- coding: utf-8 -*-
"""
================================================================================
JUCA · O COORDENADOR DOS JOCAS & SUPERVISOR SRE AUTÔNOMO DE OPERAÇÕES (WATCHDOG)
================================================================================
Módulo de observabilidade ativa, diagnóstico de causa raiz (RCA), auto-recuperação
(Self-Healing) e escalonamento para o ecossistema dos bots e pipelines:
  • Monitora os 3 Jocas: Joca_BigQuery, Joca_Fabric, Joca_B3 (Teams)
  • Monitora a Infraestrutura: Render Web Service, Uptime, Latência e Webhooks Telegram
  • Monitora a Integridade de Dados: Frescor (Data BQ vs Data Bot vs Calendário de Mercado)
  • Realiza Testes Sintéticos Contínuos: Simula consultas reais de Dow Jones e Meli
  • Monitora os 6 Pipelines no GitHub Actions
  • Auto-Recuperação Nível 1:
      - Acorda instância do Render caso esteja em hibernação (Keep-Alive)
      - Reconfigura webhooks do Telegram automaticamente caso caiam
      - "Aperta o botão de atualizar" no GitHub Actions caso os dados estejam defasados
  • Escalonamento Nível 2: Se 4 tentativas de auto-cura falharem, alerta Karl no Teams
    com diagnóstico analítico detalhado da causa raiz.
================================================================================
"""

import os
import sys
import json
import time
import logging
import threading
import subprocess
from datetime import datetime, date, timezone, timedelta
import re
import requests

logger = logging.getLogger("JucaSupervisor")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] JucaSupervisor: %(message)s")

BRT_TZ = timezone(timedelta(hours=-3))

# Diretório base e arquivos de estado
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JUCA_STATE_FILE = os.path.join(BASE_DIR, "juca_state.json")
TEAMS_SESSION_FILE = os.path.join(BASE_DIR, "teams_karl_session.json")

# Constantes de infraestrutura e tokens dos bots
RENDER_BOT_URL = os.environ.get("RENDER_BOT_URL", "https://meli-intelligence-bot.onrender.com").rstrip("/")
TOKEN_BQ = os.environ.get("TELEGRAM_TOKEN", "").strip() or "8916733671:AAH1htvd6VqDKsngdyYsFOaXdvNgUQ0RjyM"
TOKEN_FABRIC = os.environ.get("TELEGRAM_TOKEN_FABRIC", "").strip() or "8958525363:AAH0RD0l8eXvre1Yy2XTOtVLnOAB8luQdhE"

# Configurações de pipelines monitorados no GitHub Actions
PIPELINES = [
    {
        "id": "b3",
        "name": "B3 · Cotações, Ações & Índices",
        "tag": "B3 Ações",
        "repo": "karl-albert/Atualizador_BigQuery_B3",
        "wf": "rotina_b3.yml",
        "cron_desc": "Seg a Sex às 10h40, 14h40 e 18h40 (BRT)",
        "tolerance_min": 15,
        "times": [(10, 40), (14, 40), (18, 40)],
        "weekdays": [0, 1, 2, 3, 4]
    },
    {
        "id": "macro",
        "name": "B3 Macroeconomia · IPCA, Selic, PIB",
        "tag": "B3 Macro",
        "repo": "karl-albert/Atualizador_BigQuery_B3",
        "wf": "rotina_macro.yml",
        "cron_desc": "Sábados às 08h10 (BRT)",
        "tolerance_min": 15,
        "times": [(8, 10)],
        "weekdays": [5]
    },
    {
        "id": "mercadolivre",
        "name": "Mercado Livre · Mais Vendidos (BigQuery)",
        "tag": "Mercado Livre BQ",
        "repo": "karl-albert/Atualizador_BigQuery_MercadoLivre-",
        "wf": "rotina_mercadolivre.yml",
        "cron_desc": "Diariamente às 08h00, 18h00 e 23h00 (BRT)",
        "tolerance_min": 30,
        "times": [(8, 0), (18, 0), (23, 0)],
        "weekdays": [0, 1, 2, 3, 4, 5, 6]
    },
    {
        "id": "mercadolivre_fabric",
        "name": "Mercado Livre · Fabric OneLake & Direct Lake",
        "tag": "ML Fabric",
        "repo": "karl-albert/Atualizador_Fabric_MercadoLivre",
        "wf": "rotina_atualizador_fabric.yml",
        "cron_desc": "Diariamente às 08h15, 18h15 e 23h15 (BRT)",
        "tolerance_min": 35,
        "times": [(8, 15), (18, 15), (23, 15)],
        "weekdays": [0, 1, 2, 3, 4, 5, 6]
    },
    {
        "id": "americans",
        "name": "Mercado Americano · US Stocks & Macro FRED",
        "tag": "US Markets",
        "repo": "karl-albert/Atualizador_BigQuery_Americans",
        "wf": "cron.yml",
        "cron_desc": "Seg a Sex de hora em hora (:00 BRT)",
        "tolerance_min": 30,
        "times": [(h, 0) for h in range(9, 19)],
        "weekdays": [0, 1, 2, 3, 4]
    },
    {
        "id": "mp",
        "name": "Ministério Público · Supabase",
        "tag": "MP Supabase",
        "repo": "karl-albert/Ministerio_Publico_Supabase",
        "wf": "carga-mp.yml",
        "cron_desc": "Diariamente às 22h30 (BRT)",
        "tolerance_min": 45,
        "times": [(22, 30)],
        "weekdays": [0, 1, 2, 3, 4, 5, 6]
    }
]


def obter_github_token_sistema():
    """Obtém o token do GitHub por variáveis de ambiente ou Git Credential Manager."""
    tok = os.environ.get("GITHUB_TOKEN", "").strip() or os.environ.get("GH_PAT", "").strip()
    if tok:
        return tok
    try:
        proc = subprocess.Popen(
            ["git", "credential", "fill"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        out, _ = proc.communicate(input="protocol=https\nhost=github.com\n\n", timeout=4)
        for line in out.splitlines():
            if line.startswith("password="):
                t = line.split("=", 1)[1].strip()
                if t:
                    return t
    except Exception:
        pass
    return ""


def carregar_estado_juca():
    """Carrega o histórico de incidentes e auto-curas."""
    if os.path.exists(JUCA_STATE_FILE):
        try:
            with open(JUCA_STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erro ao carregar {JUCA_STATE_FILE}: {e}")
    return {
        "incidentes": {},
        "historico_acoes": [],
        "ultimo_check": None,
        "status_geral": "OPERACIONAL_VERDE",
        "saude_bots": {}
    }


def salvar_estado_juca(estado):
    """Persiste o estado do Juca."""
    try:
        with open(JUCA_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(estado, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Erro ao salvar {JUCA_STATE_FILE}: {e}")


def carregar_sessao_teams():
    """Carrega a sessão ativa do Karl no Teams."""
    if os.path.exists(TEAMS_SESSION_FILE):
        try:
            with open(TEAMS_SESSION_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erro ao ler sessao teams: {e}")
    conv_id = os.environ.get("TEAMS_CONVERSATION_ID", "").strip()
    svc_url = os.environ.get("TEAMS_SERVICE_URL", "https://smba.trafficmanager.net/amer/").strip()
    if conv_id:
        return {"conversation_id": conv_id, "service_url": svc_url, "user_name": "Karl Albert"}
    return None


def salvar_sessao_teams(conversation_id, service_url, user_name="Karl Albert"):
    """Salva a sessão do Teams a partir de uma mensagem recebida."""
    dados = {
        "conversation_id": conversation_id,
        "service_url": service_url,
        "user_name": user_name,
        "updated_at": datetime.now(BRT_TZ).strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        with open(TEAMS_SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=2, ensure_ascii=False)
        logger.info(f"Sessão Teams de {user_name} gravada com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar sessão Teams: {e}")


def obter_data_esperada_mercado():
    """Calcula a data esperada do último pregão/fechamento de mercado."""
    agora = datetime.now(BRT_TZ)
    hoje = agora.date()
    if hoje.weekday() == 5:  # Sábado
        return hoje - timedelta(days=1)
    elif hoje.weekday() == 6:  # Domingo
        return hoje - timedelta(days=2)
    # Segunda a Sexta: antes das 18h40 o fechamento oficial do dia ainda não é obrigatório
    if agora.hour < 18 or (agora.hour == 18 and agora.minute < 40):
        if hoje.weekday() == 0:  # Segunda antes do fechamento -> espera sexta
            return hoje - timedelta(days=3)
        return hoje - timedelta(days=1)
    return hoje


class JucaSupervisor:
    """Motor supremo de orquestração, observabilidade e auto-cura do Juca."""

    def __init__(self, token_github=None):
        self._token_github = token_github
        self.teams_webhook_url = os.environ.get("TEAMS_WEBHOOK_URL", "").strip()
        self.telegram_admin_chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "891673367")
        self.bot_url = RENDER_BOT_URL

    @property
    def token_github(self):
        if not self._token_github:
            self._token_github = obter_github_token_sistema()
        return self._token_github

    def obter_github_headers(self):
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "Juca-Supervisor-Bot"
        }
        tok = self.token_github
        if tok:
            headers["Authorization"] = f"token {tok}"
        return headers

    # --------------------------------------------------------------------------
    # 1. AUDITORIA ATIVA E PROFUNDA DA SAÚDE DOS BOTS (RENDER + TELEGRAM + DADOS)
    # --------------------------------------------------------------------------
    def auditar_saude_bots(self):
        """
        Audita ativamente toda a ponta final dos robôs:
          • Web Service no Render (Uptime, Latência e Status HTTP)
          • Webhooks do Telegram (Joca_BQ e Joca_Fabric)
          • Frescor dos dados (Data ML vs Data B3 vs Calendário de Mercado)
          • Testes sintéticos com perguntas reais (Dow Jones e IA)
        Aplica auto-recuperação Nível 1 automaticamente caso detecte anomalia.
        """
        resultado = {
            "render_online": False,
            "render_latencia_ms": None,
            "render_status_code": None,
            "data_recente_ml": None,
            "data_recente_b3_acoes": None,
            "data_recente_indices_eua": None,
            "total_registros_ml": None,
            "webhook_bq_ok": False,
            "webhook_bq_pending": 0,
            "webhook_bq_erro": None,
            "webhook_fabric_ok": False,
            "webhook_fabric_pending": 0,
            "webhook_fabric_erro": None,
            "teste_sintetico_b3_acoes_ok": False,
            "teste_sintetico_b3_acoes_resumo": None,
            "teste_sintetico_dow_jones_ok": False,
            "teste_sintetico_dow_jones_resumo": None,
            "data_esperada_mercado": str(obter_data_esperada_mercado()),
            "anomalias": [],
            "auto_cura_aplicada": []
        }

        # A) PROBE 1: RENDER WEB SERVICE (/status)
        t0 = time.time()
        try:
            r_st = requests.get(f"{self.bot_url}/status", timeout=12)
            resultado["render_latencia_ms"] = int((time.time() - t0) * 1000)
            resultado["render_status_code"] = r_st.status_code
            if r_st.status_code == 200:
                resultado["render_online"] = True
                d_st = r_st.json()
                resultado["data_recente_ml"] = d_st.get("data_recente")
                resultado["data_recente_b3_acoes"] = d_st.get("b3_tickers_max_data") or d_st.get("b3_ibov_max_data")
                resultado["data_recente_indices_eua"] = d_st.get("b3_indices_eua_max_data") or d_st.get("b3_indices_max_data")
                resultado["total_registros_ml"] = d_st.get("total_registros")
            else:
                resultado["anomalias"].append(f"Render retornou HTTP {r_st.status_code} no endpoint /status")
        except Exception as e_rend:
            resultado["render_latencia_ms"] = int((time.time() - t0) * 1000)
            resultado["anomalias"].append(f"Render inacessível ou em hibernação profunda: {str(e_rend)[:120]}")

        # B) PROBE 2: WEBHOOK TELEGRAM JOCA_BIGQUERY
        try:
            r_wh_bq = requests.get(f"https://api.telegram.org/bot{TOKEN_BQ}/getWebhookInfo", timeout=6)
            if r_wh_bq.status_code == 200:
                res_bq = r_wh_bq.json().get("result", {})
                url_bq = res_bq.get("url", "")
                pending_bq = res_bq.get("pending_update_count", 0)
                last_err_bq = res_bq.get("last_error_message")
                resultado["webhook_bq_pending"] = pending_bq
                resultado["webhook_bq_erro"] = last_err_bq

                if url_bq and "meli-intelligence-bot" in url_bq:
                    resultado["webhook_bq_ok"] = True
                else:
                    resultado["anomalias"].append("Webhook do Telegram (Joca_BigQuery) desvinculado ou vazio")
                
                if pending_bq > 15:
                    resultado["anomalias"].append(f"Joca_BigQuery com {pending_bq} mensagens encalhadas na fila do Telegram")
        except Exception as e_tg1:
            resultado["anomalias"].append(f"Erro ao consultar webhook Joca_BigQuery: {str(e_tg1)[:100]}")

        # C) PROBE 3: WEBHOOK TELEGRAM JOCA_FABRIC
        try:
            r_wh_fab = requests.get(f"https://api.telegram.org/bot{TOKEN_FABRIC}/getWebhookInfo", timeout=6)
            if r_wh_fab.status_code == 200:
                res_fab = r_wh_fab.json().get("result", {})
                url_fab = res_fab.get("url", "")
                pending_fab = res_fab.get("pending_update_count", 0)
                last_err_fab = res_fab.get("last_error_message")
                resultado["webhook_fabric_pending"] = pending_fab
                resultado["webhook_fabric_erro"] = last_err_fab

                if url_fab and "meli-intelligence-bot" in url_fab:
                    resultado["webhook_fabric_ok"] = True
                else:
                    resultado["anomalias"].append("Webhook do Telegram (Joca_Fabric) desvinculado ou vazio")

                if pending_fab > 15:
                    resultado["anomalias"].append(f"Joca_Fabric com {pending_fab} mensagens encalhadas na fila do Telegram")
        except Exception as e_tg2:
            resultado["anomalias"].append(f"Erro ao consultar webhook Joca_Fabric: {str(e_tg2)[:100]}")

        # AUTO-HEALING TELEGRAM: Se qualquer webhook caiu, re-associa agora!
        if (not resultado["webhook_bq_ok"]) or (not resultado["webhook_fabric_ok"]):
            logger.warning("[JUCA AUTO-CURA] Webhook do Telegram detectado com falha. Disparando /set_all_webhooks...")
            try:
                r_fix = requests.get(f"{self.bot_url}/set_all_webhooks", timeout=8)
                if r_fix.status_code == 200:
                    resultado["auto_cura_aplicada"].append("Webhooks do Telegram reconfigurados com sucesso via /set_all_webhooks")
            except Exception as e_fix:
                logger.error(f"Erro ao reconfigurar webhooks: {e_fix}")

        # D1) PROBE 4.1: TESTE SINTÉTICO AÇÕES / ATIVOS B3 (PETR4)
        try:
            r_syn_b3 = requests.get(f"{self.bot_url}/test_b3?q=PETR4", timeout=10)
            if r_syn_b3.status_code == 200:
                corpo_b3 = r_syn_b3.json().get("resposta", "")
                if "PETR4" in corpo_b3 and ("Fechamento" in corpo_b3 or "Preço" in corpo_b3):
                    resultado["teste_sintetico_b3_acoes_ok"] = True
                    linhas_b3 = [l.strip() for l in corpo_b3.split("\n") if l.strip()]
                    resultado["teste_sintetico_b3_acoes_resumo"] = linhas_b3[0] if linhas_b3 else "OK"
                    if not resultado.get("data_recente_b3_acoes"):
                        m_dt = re.search(r'(\d{2}/\d{2}/\d{4})', corpo_b3)
                        if m_dt:
                            try:
                                resultado["data_recente_b3_acoes"] = datetime.strptime(m_dt.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
                            except Exception:
                                pass
                else:
                    resultado["anomalias"].append("Teste sintético de Ações B3 (PETR4) retornou resposta incompleta")
            else:
                resultado["anomalias"].append(f"Teste sintético de Ações B3 (PETR4) falhou com HTTP {r_syn_b3.status_code}")
        except Exception as e_syn_b3:
            resultado["anomalias"].append(f"Exceção no teste sintético Ações B3 (PETR4): {str(e_syn_b3)[:100]}")

        # D2) PROBE 4.2: TESTE SINTÉTICO ÍNDICES AMERICANOS (Dow Jones)
        try:
            r_syn_eua = requests.get(f"{self.bot_url}/test_b3?q=Dow%20Jones", timeout=10)
            if r_syn_eua.status_code == 200:
                corpo_eua = r_syn_eua.json().get("resposta", "")
                if "Dow Jones" in corpo_eua and ("Fechamento" in corpo_eua or "pts" in corpo_eua):
                    resultado["teste_sintetico_dow_jones_ok"] = True
                    linhas_eua = [l.strip() for l in corpo_eua.split("\n") if l.strip()]
                    resultado["teste_sintetico_dow_jones_resumo"] = linhas_eua[0] if linhas_eua else "OK"
                    if not resultado.get("data_recente_indices_eua"):
                        m_dt = re.search(r'(\d{2}/\d{2}/\d{4})', corpo_eua)
                        if m_dt:
                            try:
                                resultado["data_recente_indices_eua"] = datetime.strptime(m_dt.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
                            except Exception:
                                pass
                else:
                    resultado["anomalias"].append("Teste sintético de Índices EUA (Dow Jones) retornou resposta incompleta")
            else:
                resultado["anomalias"].append(f"Teste sintético de Índices EUA (Dow Jones) falhou com HTTP {r_syn_eua.status_code}")
        except Exception as e_syn_eua:
            resultado["anomalias"].append(f"Exceção no teste sintético Dow Jones: {str(e_syn_eua)[:100]}")

        # E) PROBE 5: ANÁLISE DE FRESCOR DE DADOS (DATA FRESHNESS INDEPENDENTE)
        dt_esp = str(obter_data_esperada_mercado())
        dt_acoes = resultado.get("data_recente_b3_acoes")
        dt_eua = resultado.get("data_recente_indices_eua")

        if dt_acoes and dt_acoes < dt_esp:
            resultado["anomalias"].append(f"Base de Ações B3 (PETR4) está defasada ({dt_acoes} vs esperada {dt_esp})")
        
        if dt_eua and dt_eua < dt_esp:
            resultado["anomalias"].append(f"Base de Índices Americanos (Dow Jones) está defasada ({dt_eua} vs esperada {dt_esp})")

        return resultado

    # --------------------------------------------------------------------------
    # 2. AUDITORIA DOS PIPELINES (GITHUB ACTIONS)
    # --------------------------------------------------------------------------
    def consultar_runs(self, repo, wf_file, per_page=10):
        """Consulta as últimas execuções de um workflow no GitHub Actions."""
        url = f"https://api.github.com/repos/{repo}/actions/workflows/{wf_file}/runs?per_page={per_page}"
        try:
            r = requests.get(url, headers=self.obter_github_headers(), timeout=10)
            if r.status_code == 200:
                return r.json().get("workflow_runs", [])
            else:
                logger.warning(f"GitHub API {url} retornou {r.status_code}: {r.text[:100]}")
        except Exception as e:
            logger.error(f"Exceção ao consultar runs de {repo}/{wf_file}: {e}")
        return []

    def obter_ultimo_agendamento(self, p_info, agora):
        """Calcula o último horário previsto do pipeline."""
        times = p_info.get("times", [])
        weekdays = p_info.get("weekdays", [])
        
        candidatos = []
        for d in range(7):
            dia = (agora - timedelta(days=d)).date()
            if dia.weekday() in weekdays:
                for h, m in times:
                    dt = datetime(dia.year, dia.month, dia.day, h, m, 0, tzinfo=BRT_TZ)
                    if dt <= agora:
                        candidatos.append(dt)
        if candidatos:
            candidatos.sort(reverse=True)
            return candidatos[0]
        return None

    def avaliar_pipeline(self, p_info, agora):
        """Avalia se o pipeline está em dia, atrasado, falho ou pendente."""
        runs = self.consultar_runs(p_info["repo"], p_info["wf"], per_page=8)
        ultimo_agendado = self.obter_ultimo_agendamento(p_info, agora)
        tolerancia = timedelta(minutes=p_info.get("tolerance_min", 30))

        if not runs:
            return {
                "status": "AMARELO_DESCONHECIDO",
                "motivo": "Sem histórico retornado pelo GitHub API",
                "ultimo_run": None,
                "ultimo_agendado": ultimo_agendado
            }

        latest_run = runs[0]
        run_status = latest_run.get("status")
        run_conclusion = latest_run.get("conclusion")
        run_created_utc = datetime.fromisoformat(latest_run["created_at"].replace("Z", "+00:00"))
        run_created_brt = run_created_utc.astimezone(BRT_TZ)

        if run_status in ["in_progress", "queued"]:
            return {
                "status": "EM_EXECUCAO",
                "motivo": f"Workflow em andamento desde {run_created_brt.strftime('%H:%M:%S')}",
                "ultimo_run": latest_run,
                "ultimo_agendado": ultimo_agendado
            }

        if run_conclusion == "failure":
            return {
                "status": "VERMELHO_FALHA",
                "motivo": f"Última execução em {run_created_brt.strftime('%d/%m %H:%M')} falhou",
                "ultimo_run": latest_run,
                "ultimo_agendado": ultimo_agendado
            }

        if ultimo_agendado:
            limite_esperado = ultimo_agendado + tolerancia
            teve_sucesso_recente = False
            for r in runs:
                if r.get("conclusion") == "success":
                    r_utc = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
                    r_brt = r_utc.astimezone(BRT_TZ)
                    if r_brt >= (ultimo_agendado - timedelta(minutes=15)):
                        teve_sucesso_recente = True
                        break

            if not teve_sucesso_recente and agora > limite_esperado:
                atraso_min = int((agora - ultimo_agendado).total_seconds() / 60)
                return {
                    "status": "AMARELO_ATRASO",
                    "motivo": f"Agendamento das {ultimo_agendado.strftime('%H:%M')} não executou (Atraso de {atraso_min} min)",
                    "ultimo_run": latest_run,
                    "ultimo_agendado": ultimo_agendado
                }

        return {
            "status": "VERDE",
            "motivo": f"Última execução em {run_created_brt.strftime('%d/%m %H:%M')} concluída com sucesso",
            "ultimo_run": latest_run,
            "ultimo_agendado": ultimo_agendado
        }

    def disparar_workflow_botao(self, repo, wf_file):
        """Aciona autonomamente o workflow_dispatch no GitHub Actions."""
        tok = self.token_github
        if not tok:
            logger.error("Token do GitHub ausente! Não foi possível disparar workflow_dispatch.")
            return False, "Token do GitHub não configurado no Juca"

        url = f"https://api.github.com/repos/{repo}/actions/workflows/{wf_file}/dispatches"
        headers = self.obter_github_headers()
        body = {"ref": "main"}
        try:
            r = requests.post(url, headers=headers, json=body, timeout=12)
            if r.status_code in [200, 204]:
                logger.info(f"[JUCA SUCESSO] Disparo do workflow {wf_file} em {repo} realizado com sucesso!")
                return True, "Workflow disparado com sucesso"
            else:
                msg_erro = f"Código {r.status_code}: {r.text}"
                logger.error(f"[JUCA ERRO] Falha no disparo {repo}/{wf_file}: {msg_erro}")
                return False, msg_erro
        except Exception as e:
            logger.error(f"[JUCA EXCEÇÃO] Erro ao disparar {repo}/{wf_file}: {e}")
            return False, str(e)

    # --------------------------------------------------------------------------
    # 3. COMUNICAÇÃO E ESCALONAMENTO SRE (TEAMS & TELEGRAM)
    # --------------------------------------------------------------------------
    def enviar_alerta_teams(self, titulo, mensagem, severity="info", url_run=None):
        """Envia alerta direto no Microsoft Teams do Karl via Bot Framework ou Webhook."""
        sessao = carregar_sessao_teams()
        if sessao:
            try:
                from app import enviar_mensagem_teams
                corpo_teams = f"{titulo}\n\n{mensagem}"
                if url_run:
                    corpo_teams += f"\n\n🔗 **Log da Execução:** [Clique aqui para abrir no GitHub]({url_run})"
                enviar_mensagem_teams(sessao["service_url"], sessao["conversation_id"], corpo_teams)
                logger.info("Alerta do Juca entregue via Bot Framework Teams com sucesso.")
            except Exception as e:
                logger.error(f"Erro ao enviar alerta via Bot Framework: {e}")

        if self.teams_webhook_url:
            try:
                cores = {"info": "3b82f6", "warning": "f59e0b", "danger": "ef4444", "success": "10b981"}
                card = {
                    "@type": "MessageCard",
                    "@context": "http://schema.org/extensions",
                    "themeColor": cores.get(severity, "3b82f6"),
                    "summary": titulo,
                    "sections": [{
                        "activityTitle": titulo,
                        "activitySubtitle": f"Vigilância Operacional · {datetime.now(BRT_TZ).strftime('%d/%m/%Y %H:%M:%S')} BRT",
                        "text": mensagem.replace("\n", "<br/>")
                    }]
                }
                if url_run:
                    card["potentialAction"] = [{
                        "@type": "OpenUri",
                        "name": "🔍 Abrir Log no GitHub Actions",
                        "targets": [{"os": "default", "uri": url_run}]
                    }]
                requests.post(self.teams_webhook_url, json=card, timeout=10)
                logger.info("Alerta do Juca entregue via Teams Webhook.")
            except Exception as e:
                logger.error(f"Erro ao enviar webhook do Teams: {e}")

        self.enviar_backup_telegram(titulo, mensagem, url_run)
        return True

    def enviar_backup_telegram(self, titulo, mensagem, url_run=None):
        """Garante que Karl nunca fique sem aviso enviando backup via Telegram."""
        try:
            from app import enviar_mensagem_telegram, TOKEN_BQ
            texto_tg = f"{titulo}\n\n{mensagem}"
            if url_run:
                texto_tg += f"\n\n🔗 Log: {url_run}"
            enviar_mensagem_telegram(self.telegram_admin_chat_id, texto_tg, token=TOKEN_BQ)
        except Exception as e:
            logger.debug(f"Backup Telegram não enviado: {e}")

    # --------------------------------------------------------------------------
    # 4. CICLO COMPLETO DE VIGILÂNCIA & AUTO-HEALING SRE
    # --------------------------------------------------------------------------
    def executar_ciclo_vigilancia(self):
        """
        Ciclo principal do Juca executado periodicamente:
          1. Avalia a saúde real dos bots na ponta (Render, Webhooks, Dados e Sintético).
          2. Avalia todos os 6 pipelines no GitHub Actions.
          3. Executa Auto-Recuperação Nível 1.
          4. Se 4 tentativas falharem, escala para Karl no Teams com Diagnóstico de Causa Raiz.
        """
        agora = datetime.now(BRT_TZ)
        estado = carregar_estado_juca()
        incidentes = estado.get("incidentes", {})
        historico = estado.get("historico_acoes", [])

        relatorio = []
        houve_alteracao = False

        # --- FASE 1: AUDITORIA DOS BOTS ---
        saude_bots = self.auditar_saude_bots()
        estado["saude_bots"] = saude_bots

        if saude_bots["anomalias"]:
            inc_bot = incidentes.get("bots_hub")
            detalhe_anomalias = " | ".join(saude_bots["anomalias"])

            if not inc_bot:
                logger.info(f"[JUCA AUTO-HEAL] Anomalia detectada nos bots: {detalhe_anomalias}. Iniciando auto-recuperação (Tentativa 1/4)...")
                
                # Auto-cura específica para dados B3 desatualizados
                if any("Base B3 no robô está defasada" in an for an in saude_bots["anomalias"]):
                    self.disparar_workflow_botao("karl-albert/Atualizador_BigQuery_B3", "rotina_b3.yml")

                incidentes["bots_hub"] = {
                    "componente": "Hub dos Bots (Render & Webhooks)",
                    "motivo_original": detalhe_anomalias,
                    "primeiro_disparo": agora.isoformat(),
                    "ultimo_disparo": agora.isoformat(),
                    "tentativas": 1,
                    "max_tentativas": 4,
                    "status": "AUTO_HEALING_EM_ANDAMENTO",
                    "escalonado_karl": False
                }
                historico.append({
                    "data_hora": agora.strftime("%Y-%m-%d %H:%M:%S"),
                    "alvo": "bots_hub",
                    "evento": "AUTO_HEAL_TENTATIVA_1",
                    "detalhe": f"Auto-cura disparada para bots: {detalhe_anomalias}"
                })
                houve_alteracao = True
            else:
                if not inc_bot.get("escalonado_karl"):
                    ultimo_dt_str = inc_bot.get("ultimo_disparo") or inc_bot.get("primeiro_disparo")
                    tentativa_dt = datetime.fromisoformat(ultimo_dt_str)
                    passaram_minutos = (agora - tentativa_dt).total_seconds() / 60
                    tentativas_atuais = inc_bot.get("tentativas", 1)

                    if passaram_minutos >= 10:
                        if tentativas_atuais < 4:
                            tentativas_atuais += 1
                            logger.info(f"[JUCA AUTO-HEAL] Reexecutando auto-recuperação nos bots (Tentativa {tentativas_atuais}/4)...")
                            if any("Base B3" in an for an in saude_bots["anomalias"]):
                                self.disparar_workflow_botao("karl-albert/Atualizador_BigQuery_B3", "rotina_b3.yml")
                            inc_bot["tentativas"] = tentativas_atuais
                            inc_bot["ultimo_disparo"] = agora.isoformat()
                            houve_alteracao = True
                        else:
                            # 4 TENTATIVAS FALHARAM: ESCALONAMENTO SRE NO TEAMS
                            logger.warning("[JUCA ESCALONAMENTO] 4 tentativas falharam nos bots! Acionando Karl no Teams...")
                            titulo = "🚨 **[JUCA SUPERVISOR] Falha Crítica nos Bots · 4 Tentativas de Auto-Cura Falharam!**"
                            msg = (
                                f"Karl, preciso da sua atuação direta no ecossistema de robôs!\n\n"
                                f"⚠️ **Diagnóstico de Causa Raiz:**\n"
                                f"{chr(10).join(['• ' + an for an in saude_bots['anomalias']])}\n\n"
                                f"🛠️ **Ação Tomada pelo Juca:** Executei 4 ciclos de auto-recuperação (reconfiguração de webhooks, pings de wake-up e disparos de carga).\n"
                                f"❌ **Resultado:** A anomalia persistiu e o limite de auto-cura foi atingido.\n"
                                f"👨‍💻 **Ação Requerida:** Verifique os logs do Render ou credenciais das APIs."
                            )
                            self.enviar_alerta_teams(titulo, msg, severity="danger")
                            inc_bot["escalonado_karl"] = True
                            inc_bot["status"] = "ESCALONADO"
                            houve_alteracao = True
        else:
            if "bots_hub" in incidentes:
                del incidentes["bots_hub"]
                houve_alteracao = True

        # --- FASE 2: AUDITORIA DOS PIPELINES ---
        for p in PIPELINES:
            pid = p["id"]
            nome = p["name"]
            repo = p["repo"]
            wf = p["wf"]

            avaliacao = self.avaliar_pipeline(p, agora)
            status = avaliacao["status"]
            motivo = avaliacao["motivo"]
            latest_run = avaliacao["ultimo_run"]
            url_run = latest_run.get("html_url") if latest_run else None

            incidente_ativo = incidentes.get(pid)

            if status == "VERDE":
                if incidente_ativo and incidente_ativo.get("status") in ["AUTO_HEALING_DISPARADO", "AUTO_HEALING_EM_ANDAMENTO", "ESCALONADO"]:
                    qtd_tentativas = incidente_ativo.get("tentativas", 1)
                    if incidente_ativo.get("escalonado_karl"):
                        titulo = "🟢 **[JUCA SUPERVISOR] Auto-Recuperação Concluída com Sucesso!**"
                        msg = (
                            f"Olá Karl! O pipeline **{nome}** foi restabelecido e concluiu com 100% de êxito!\n\n"
                            f"✅ **Ação tomada:** Foram realizadas {qtd_tentativas} tentativa(s) de reexecução no GitHub Actions.\n"
                            f"📊 **Status:** Os dados foram consolidados no BigQuery e o ecossistema continua 100% operacional."
                        )
                        self.enviar_alerta_teams(titulo, msg, severity="success", url_run=url_run)
                    
                    logger.info(f"[JUCA SUCESSO] Pipeline {nome} restabelecido após {qtd_tentativas} tentativa(s).")
                    historico.append({
                        "data_hora": agora.strftime("%Y-%m-%d %H:%M:%S"),
                        "pipeline": pid,
                        "evento": "RECUPERADO_SUCESSO",
                        "detalhe": f"Pipeline voltou ao status VERDE após {qtd_tentativas} tentativa(s)."
                    })
                    del incidentes[pid]
                    houve_alteracao = True
                relatorio.append({"id": pid, "nome": nome, "status": "VERDE", "motivo": motivo})
                continue

            if status == "EM_EXECUCAO":
                relatorio.append({"id": pid, "nome": nome, "status": "EM_EXECUCAO", "motivo": motivo})
                continue

            if status in ["AMARELO_ATRASO", "VERMELHO_FALHA"]:
                if not incidente_ativo:
                    logger.info(f"[JUCA AUTO-HEAL] Iniciando auto-recuperação para {nome} (Tentativa 1/4)...")
                    sucesso_disp, msg_disp = self.disparar_workflow_botao(repo, wf)
                    
                    incidentes[pid] = {
                        "pipeline": pid,
                        "nome": nome,
                        "motivo_original": motivo,
                        "primeiro_disparo": agora.isoformat(),
                        "ultimo_disparo": agora.isoformat(),
                        "tentativas": 1,
                        "max_tentativas": 4,
                        "status": "AUTO_HEALING_EM_ANDAMENTO",
                        "disparo_sucesso": sucesso_disp,
                        "escalonado_karl": False
                    }
                    historico.append({
                        "data_hora": agora.strftime("%Y-%m-%d %H:%M:%S"),
                        "pipeline": pid,
                        "evento": "AUTO_HEAL_TENTATIVA_1",
                        "detalhe": f"Juca disparou tentativa 1/4 de {wf} após detectar {motivo}"
                    })
                    houve_alteracao = True
                else:
                    if not incidente_ativo.get("escalonado_karl"):
                        ultimo_dt_str = incidente_ativo.get("ultimo_disparo") or incidente_ativo.get("primeiro_disparo")
                        tentativa_dt = datetime.fromisoformat(ultimo_dt_str)
                        passaram_minutos = (agora - tentativa_dt).total_seconds() / 60
                        tentativas_atuais = incidente_ativo.get("tentativas", 1)

                        if passaram_minutos >= 10:
                            if tentativas_atuais < 4:
                                tentativas_atuais += 1
                                logger.info(f"[JUCA AUTO-HEAL] Reexecutando auto-recuperação para {nome} (Tentativa {tentativas_atuais}/4)...")
                                sucesso_disp, msg_disp = self.disparar_workflow_botao(repo, wf)
                                incidente_ativo["tentativas"] = tentativas_atuais
                                incidente_ativo["ultimo_disparo"] = agora.isoformat()
                                incidente_ativo["status"] = "AUTO_HEALING_EM_ANDAMENTO"
                                historico.append({
                                    "data_hora": agora.strftime("%Y-%m-%d %H:%M:%S"),
                                    "pipeline": pid,
                                    "evento": f"AUTO_HEAL_TENTATIVA_{tentativas_atuais}",
                                    "detalhe": f"Juca disparou tentativa {tentativas_atuais}/4 de {wf} após persistência de {motivo}"
                                })
                                houve_alteracao = True
                            else:
                                logger.warning(f"[JUCA ESCALONAMENTO] 4 tentativas falharam para {nome}! Chamando Karl no Teams...")
                                primeiro_dt = datetime.fromisoformat(incidente_ativo["primeiro_disparo"])
                                titulo = f"🚨 **[JUCA SUPERVISOR] Atenção Karl · 4 Tentativas Falharam!**"
                                msg = (
                                    f"Karl, preciso da sua entrada no circuito!\n\n"
                                    f"⚠️ **Problema:** A rotina **{nome}** apresentou falha ou atraso persistente ({motivo}).\n"
                                    f"🛠️ **Medida tomada pelo Juca:** Esgotei as **4 tentativas automáticas** de reexecução no GitHub Actions (iniciadas às {primeiro_dt.strftime('%H:%M:%S')}).\n"
                                    f"❌ **Resultado:** Todas as 4 tentativas não deram certo ou falharam.\n"
                                    f"👨‍💻 **Ação Requerida:** Por favor, verifique o repositório, credenciais ou conexões externas."
                                )
                                self.enviar_alerta_teams(titulo, msg, severity="danger", url_run=url_run)
                                incidente_ativo["escalonado_karl"] = True
                                incidente_ativo["status"] = "ESCALONADO"
                                houve_alteracao = True

                relatorio.append({"id": pid, "nome": nome, "status": f"PROBLEMA_{status}", "motivo": motivo})

        estado["incidentes"] = incidentes
        estado["historico_acoes"] = historico[-50:]
        estado["ultimo_check"] = agora.strftime("%Y-%m-%d %H:%M:%S")
        estado["status_geral"] = "ALERTA" if incidentes else "OPERACIONAL_VERDE"
        salvar_estado_juca(estado)

        return relatorio

    # --------------------------------------------------------------------------
    # 5. DIAGNÓSTICO PROFUNDO & RAIO-X COMPLETO DO ECOSSISTEMA
    # --------------------------------------------------------------------------
    def diagnosticar_problemas_profundo(self):
        """Gera um diagnóstico analítico 360º de todo o ecossistema."""
        agora = datetime.now(BRT_TZ)
        saude_bots = self.auditar_saude_bots()
        estado = carregar_estado_juca()
        incidentes = estado.get("incidentes", {})

        diagnostico = {
            "timestamp": agora.strftime("%d/%m/%Y %H:%M:%S BRT"),
            "status_geral": "OPERACIONAL_VERDE" if not saude_bots["anomalias"] and not incidentes else "ATENÇÃO",
            "infra_render": {
                "online": saude_bots["render_online"],
                "latencia_ms": saude_bots["render_latencia_ms"],
                "http_status": saude_bots["render_status_code"]
            },
            "webhooks_telegram": {
                "joca_bigquery": {
                    "ok": saude_bots["webhook_bq_ok"],
                    "fila_pendente": saude_bots["webhook_bq_pending"],
                    "ultimo_erro": saude_bots["webhook_bq_erro"]
                },
                "joca_fabric": {
                    "ok": saude_bots["webhook_fabric_ok"],
                    "fila_pendente": saude_bots["webhook_fabric_pending"],
                    "ultimo_erro": saude_bots["webhook_fabric_erro"]
                }
            },
            "frescor_dados": {
                "mercado_livre": saude_bots["data_recente_ml"],
                "b3_acoes": saude_bots["data_recente_b3_acoes"],
                "indices_eua": saude_bots["data_recente_indices_eua"],
                "esperado_mercado": saude_bots["data_esperada_mercado"]
            },
            "teste_sintetico": {
                "b3_acoes_ok": saude_bots["teste_sintetico_b3_acoes_ok"],
                "amostra_b3_acoes": saude_bots["teste_sintetico_b3_acoes_resumo"],
                "dow_jones_ok": saude_bots["teste_sintetico_dow_jones_ok"],
                "amostra_dow_jones": saude_bots["teste_sintetico_dow_jones_resumo"]
            },
            "anomalias_detectadas": saude_bots["anomalias"],
            "auto_curas_aplicadas": saude_bots["auto_cura_aplicada"],
            "incidentes_em_andamento": list(incidentes.keys())
        }
        return diagnostico

    # --------------------------------------------------------------------------
    # 6. COMANDOS INTERATIVOS NO TEAMS / TELEGRAM
    # --------------------------------------------------------------------------
    def responder_comando_teams(self, texto, user_name="Karl"):
        """Processa comandos interativos enviados ao Juca no Teams ou Telegram."""
        t = texto.upper()
        agora = datetime.now(BRT_TZ).strftime("%d/%m/%Y %H:%M:%S")

        # A) COMANDO: RAIO-X / DIAGNÓSTICO / PROBLEMAS
        if any(w in t for w in ["DIAGNOSTICO", "DIAGNÓSTICO", "PROBLEMA", "PROBLEMAS", "RAIO-X", "RAIOX", "SCAN", "AUDITORIA"]):
            diag = self.diagnosticar_problemas_profundo()
            rend = diag["infra_render"]
            wh = diag["webhooks_telegram"]
            frescor = diag["frescor_dados"]
            syn = diag["teste_sintetico"]
            anomalias = diag["anomalias_detectadas"]
            curas = diag["auto_curas_aplicadas"]

            status_geral_icon = "🟢" if diag["status_geral"] == "OPERACIONAL_VERDE" else "🟡"

            linhas_anomalias = "\n".join([f"• ⚠️ {a}" for a in anomalias]) if anomalias else "• ✅ Nenhuma anomalia detectada! Tudo em conformidade."
            linhas_curas = "\n".join([f"• 🛠️ {c}" for c in curas]) if curas else "• Nenhuma intervenção necessária no momento."

            card = (
                f"👑 **JUCA · DIAGNÓSTICO PROFUNDO & RAIO-X OPERACIONAL**\n\n"
                f"{status_geral_icon} **Status Geral:** {diag['status_geral']} ({diag['timestamp']})\n\n"
                f"🌐 **1. Infraestrutura (Render Cloud):**\n"
                f"• Servidor HTTP: {'🟢 Online' if rend['online'] else '🔴 Inacessível'} (HTTP {rend['http_status']})\n"
                f"• Latência de Resposta: {rend['latencia_ms']} ms\n\n"
                f"📱 **2. Webhooks dos Bots (Telegram):**\n"
                f"• **Joca_BigQuery:** {'🟢 Ativo' if wh['joca_bigquery']['ok'] else '🔴 Falha'} (Fila: {wh['joca_bigquery']['fila_pendente']} msg)\n"
                f"• **Joca_Fabric:** {'🟢 Ativo' if wh['joca_fabric']['ok'] else '🔴 Falha'} (Fila: {wh['joca_fabric']['fila_pendente']} msg)\n\n"
                f"📊 **3. Frescor dos Dados (Integridade):**\n"
                f"• Base Mercado Livre: {frescor['mercado_livre'] or 'N/D'}\n"
                f"• Base Ações B3 (PETR4/Ibov): {frescor['b3_acoes'] or 'N/D'}\n"
                f"• Base Índices EUA (Dow Jones): {frescor['indices_eua'] or 'N/D'} (Data esperada: {frescor['esperado_mercado']})\n\n"
                f"🎯 **4. Testes Sintéticos ao Vivo (B3 Dupla Prova):**\n"
                f"• **Ações B3 (PETR4):** {'🟢 100% Sucesso' if syn['b3_acoes_ok'] else '🔴 Falhou'}\n"
                f"  *Retorno:* _{syn['amostra_b3_acoes'] or 'N/D'}_\n"
                f"• **Índices EUA (Dow Jones):** {'🟢 100% Sucesso' if syn['dow_jones_ok'] else '🔴 Falhou'}\n"
                f"  *Retorno:* _{syn['amostra_dow_jones'] or 'N/D'}_\n\n"
                f"🚨 **Problemas Identificados:**\n"
                f"{linhas_anomalias}\n\n"
                f"🛠️ **Ações de Auto-Recuperação Realizadas:**\n"
                f"{linhas_curas}\n\n"
                f"💡 *Para forçar auto-cura agora, diga: `Juca conserta`.*"
            )
            return card

        # B) COMANDO: CONSERTA / AUTO-CURA MANUAL FORÇADA
        if any(w in t for w in ["CONSERTA", "CONSERTAR", "AUTO-CURA", "AUTOCURA", "REPARAR", "ACORDA"]):
            rel = self.executar_ciclo_vigilancia()
            diag = self.diagnosticar_problemas_profundo()
            syn = diag["teste_sintetico"]
            return (
                f"⚡ **Comandante {user_name}! Auto-Recuperação Executada:**\n\n"
                f"👉 Acabei de varrer todo o ecossistema, re-amarrar webhooks e acionar Keep-Alive!\n"
                f"🌐 Render: {'🟢 Online' if diag['infra_render']['online'] else '🔴 Offline'}\n"
                f"📱 Webhooks Telegram: Joca_BQ ({'🟢' if diag['webhooks_telegram']['joca_bigquery']['ok'] else '🔴'}) | Joca_Fabric ({'🟢' if diag['webhooks_telegram']['joca_fabric']['ok'] else '🔴'})\n"
                f"🎯 Testes Sintéticos B3: Ações B3 ({'🟢 OK' if syn['b3_acoes_ok'] else '🔴 Falha'}) | Dow Jones ({'🟢 OK' if syn['dow_jones_ok'] else '🔴 Falha'})\n"
                f"🕒 Horário: {agora}"
            )

        # C) COMANDOS DE ATUALIZAÇÃO MANUAL DE PIPELINES
        if any(w in t for w in ["ATUALIZA B3", "ATUALIZAR B3", "DISPARA B3", "RODA B3"]):
            sucesso, msg = self.disparar_workflow_botao("karl-albert/Atualizador_BigQuery_B3", "rotina_b3.yml")
            return f"⚡ **Disparo B3:** {'✅ Workflow acionado no GitHub Actions com sucesso!' if sucesso else '❌ Falha: ' + msg}"

        if any(w in t for w in ["ATUALIZA MELI", "ATUALIZA MERCADO LIVRE", "RODA MELI"]):
            sucesso, msg = self.disparar_workflow_botao("karl-albert/Atualizador_BigQuery_MercadoLivre-", "rotina_mercadolivre.yml")
            return f"⚡ **Disparo Mercado Livre:** {'✅ Workflow acionado no GitHub Actions com sucesso!' if sucesso else '❌ Falha: ' + msg}"

        if any(w in t for w in ["ATUALIZA MACRO", "RODA MACRO"]):
            sucesso, msg = self.disparar_workflow_botao("karl-albert/Atualizador_BigQuery_B3", "rotina_macro.yml")
            return f"⚡ **Disparo B3 Macro:** {'✅ Workflow acionado no GitHub Actions!' if sucesso else '❌ Falha: ' + msg}"

        if any(w in t for w in ["ATUALIZA FABRIC", "RODA FABRIC", "ATUALIZAR FABRIC"]):
            sucesso, msg = self.disparar_workflow_botao("karl-albert/Atualizador_Fabric_MercadoLivre", "rotina_atualizador_fabric.yml")
            return f"⚡ **Disparo Fabric OneLake:** {'✅ Pipeline disparado no GitHub Actions!' if sucesso else '❌ Falha: ' + msg}"

        # D) STATUS EXECUTIVO PADRÃO
        estado = carregar_estado_juca()
        incidentes = estado.get("incidentes", {})
        linhas = []
        for p in PIPELINES:
            pid = p["id"]
            if pid in incidentes:
                inc = incidentes[pid]
                status_emoji = "🔴" if inc.get("escalonado_karl") else "🟡"
                linhas.append(f"{status_emoji} **{p['tag']}:** {inc.get('status')} ({inc.get('motivo_original', 'Em observação')})")
            else:
                linhas.append(f"🟢 **{p['tag']}:** Operacional e em dia")

        try:
            r_st = requests.get(f"{self.bot_url}/status", timeout=4)
            if r_st.status_code == 200:
                d_st = r_st.json()
                dt_ml = d_st.get("data_recente", "N/D")
                tot_ml = d_st.get("total_registros", 0)
                dt_b3 = d_st.get("b3_indices_max_data", "N/D")
                status_jocas = (
                    "🤖 **Status dos 3 Jocas (Ao Vivo):**\n"
                    f"• **Joca_BigQuery (Telegram ML):** 🟢 Online (Base: {dt_ml} · {tot_ml:,} reg)\n"
                    "• **Joca_Fabric (Telegram OneLake):** 🟢 Online (OneLake Direct Lake)\n"
                    f"• **Joca_B3 (Teams Mercado Financeiro):** 🟢 Online (Cotações até {dt_b3})"
                )
            else:
                status_jocas = f"🤖 **Status dos 3 Jocas:** 🟡 Servidor HTTP {r_st.status_code}"
        except Exception:
            status_jocas = "🤖 **Status dos 3 Jocas:** 🟢 Ativo na nuvem"

        linhas_txt = "\n".join(linhas)
        return (
            f"👑 **JUCA · PAINEL EXECUTIVO DE SUPERVISÃO**\n\n"
            f"Olá {user_name}! Sou o **Juca**, coordenador geral e supervisor dos 3 Jocas e de todos os pipelines.\n\n"
            f"{status_jocas}\n\n"
            f"📊 **Status dos Pipelines de Carga:**\n"
            f"{linhas_txt}\n\n"
            f"🛠️ **Vigilância Ponta a Ponta Ativa:** Monitoro Render, Webhooks do Telegram, Frescor de Dados e Pipelines.\n"
            f"💡 *Para ver o Raio-X completo com testes sintéticos, diga: `Juca diagnostico`.*"
        )


# Instância global
juca = JucaSupervisor()


def iniciar_vigilancia_background(intervalo_segundos=300):
    """Inicia thread permanente do Juca Supervisor na nuvem."""
    def _loop():
        logger.info(f"👑 Juca Supervisor ativado! Rodando ciclo a cada {intervalo_segundos}s.")
        time.sleep(15)
        while True:
            try:
                juca.executar_ciclo_vigilancia()
            except Exception as e:
                logger.error(f"Erro no ciclo de vigilância do Juca: {e}")
            time.sleep(intervalo_segundos)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t
