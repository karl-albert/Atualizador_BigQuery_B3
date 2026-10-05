# -*- coding: utf-8 -*-
"""
================================================================================
JUCA · O CHEFE DOS JOCAS & SUPERVISOR AUTÔNOMO DE OPERAÇÕES (SRE WATCHDOG)
================================================================================
Módulo de orquestração autônoma, auto-recuperação (Self-Healing) e escalonamento
para o ecossistema de bots e pipelines:
  • Monitora os 3 Jocas: Joca_BigQuery, Joca_Fabric, Joca_B3
  • Monitora os Pipelines: B3 Cotações, B3 Macro, Mercado Livre, Americans, MP
  • Auto-Recuperação (Nível 1): Detecta atraso, pendência ou travamento e
    "aperta o botão de atualizar do APP" chamando workflow_dispatch no GitHub.
  • Escalonamento (Nível 2): Se a medida tomada falhar ou o problema persistir,
    alerta imediatamente Karl no Microsoft Teams explicando o que foi tentado
    e solicitando que Karl entre no circuito.
================================================================================
"""

import os
import sys
import json
import time
import logging
import threading
import subprocess
from datetime import datetime, timezone, timedelta
import requests

logger = logging.getLogger("JucaSupervisor")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] JucaSupervisor: %(message)s")

BRT_TZ = timezone(timedelta(hours=-3))

# Diretório base
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JUCA_STATE_FILE = os.path.join(BASE_DIR, "juca_state.json")
TEAMS_SESSION_FILE = os.path.join(BASE_DIR, "teams_karl_session.json")

# Configurações de pipelines monitorados
PIPELINES = [
    {
        "id": "b3",
        "name": "B3 · Cotações, Ações & Índices",
        "tag": "B3 Ações",
        "repo": "karl-albert/Atualizador_BigQuery_B3",
        "wf": "rotina_b3.yml",
        "cron_desc": "Seg a Sex às 10h40, 14h40 e 18h40 (BRT)",
        "tolerance_min": 10,
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
        "tolerance_min": 10,
        "times": [(8, 10)],
        "weekdays": [5]
    },
    {
        "id": "mercadolivre",
        "name": "Mercado Livre · Mais Vendidos & Inteligência",
        "tag": "Mercado Livre",
        "repo": "karl-albert/Atualizador_BigQuery_MercadoLivre-",
        "wf": "rotina_mercadolivre.yml",
        "cron_desc": "Diariamente às 08h00, 18h00 e 23h00 (BRT)",
        "tolerance_min": 30,
        "times": [(8, 0), (18, 0), (23, 0)],
        "weekdays": [0, 1, 2, 3, 4, 5, 6]
    },
    {
        "id": "americans",
        "name": "Mercado Americano · US Stocks & Macro FRED",
        "tag": "US Markets",
        "repo": "karl-albert/Atualizador_BigQuery_Americans",
        "wf": "cron.yml", # ⚠️ Confirme se no GitHub deste repo o arquivo não se chama 'rotina_americans.yml' ou similar
        "cron_desc": "Seg a Sex de hora em hora (:00 BRT)",
        "tolerance_min": 25,
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
        "status_geral": "OPERACIONAL_VERDE"
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


class JucaSupervisor:
    """Motor de orquestração do Juca."""

    def __init__(self, token_github=None):
        self._token_github = token_github
        self.teams_webhook_url = os.environ.get("TEAMS_WEBHOOK_URL", "").strip()
        self.telegram_admin_chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "891673367")

    @property
    def token_github(self):
        if not self._token_github:
            self._token_github = obter_github_token_sistema()
        return self._token_github

    def obter_github_headers(self):
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "Juca-Supervisor-Bot",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        tok = self.token_github
        if tok:
            # Modificado de 'token {tok}' para 'Bearer {tok}' que é o padrão correto
            headers["Authorization"] = f"Bearer {tok}"
        return headers

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
        """
        Avalia se o pipeline está em dia, atrasado, falho ou pendente.
        """
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

        # 1. Se estiver rodando agora
        if run_status in ["in_progress", "queued"]:
            return {
                "status": "EM_EXECUCAO",
                "motivo": f"Workflow em andamento desde {run_created_brt.strftime('%H:%M:%S')}",
                "ultimo_run": latest_run,
                "ultimo_agendado": ultimo_agendado
            }

        # 2. Se a última execução falhou
        if run_conclusion == "failure":
            return {
                "status": "VERMELHO_FALHA",
                "motivo": f"Última execução em {run_created_brt.strftime('%d/%m %H:%M')} falhou com status 'failure'",
                "ultimo_run": latest_run,
                "ultimo_agendado": ultimo_agendado
            }

        # 3. Verifica atraso em relação ao cron programado
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
                    "motivo": f"Agendamento das {ultimo_agendado.strftime('%H:%M')} não executou (Atraso de {atraso_min} min, tolerância de {p_info.get('tolerance_min')} min)",
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
        """
        Executa a ação autônoma: clica no botão de atualizar do APP via API do GitHub Actions.
        """
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

    def enviar_alerta_teams(self, titulo, mensagem, severity="info", url_run=None):
        """
        Envia alerta direto no Microsoft Teams do Karl via Bot Framework ou Webhook.
        """
        # 1. Envio Direto via Bot Framework (1-on-1 com Karl no Teams)
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

        # 2. Envio via Webhook de Canal (se configurado)
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

        # 3. Backup no Telegram para Karl
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

    def executar_ciclo_vigilancia(self):
        """
        Ciclo principal do Juca:
          1. Avalia todos os pipelines.
          2. Auto-recuperação Nível 1: clica no botão se houver atraso/pendência.
          3. Escalonamento Nível 2: se já tomou a medida e não funcionou, chama Karl no Teams!
        """
        agora = datetime.now(BRT_TZ)
        estado = carregar_estado_juca()
        incidentes = estado.get("incidentes", {})
        historico = estado.get("historico_acoes", [])

        relatorio = []
        houve_alteracao = False

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

            # Caso 1: Tudo Verde e Normal
            # Caso 1: Tudo Verde e Normal
            if status == "VERDE":
                if incidente_ativo and incidente_ativo.get("status") in ["AUTO_HEALING_DISPARADO", "AUTO_HEALING_EM_ANDAMENTO", "ESCALONADO"]:
                    qtd_tentativas = incidente_ativo.get("tentativas", 1)
                    if incidente_ativo.get("escalonado_karl"):
                        # Se Karl já havia sido chamado no Teams, avisa que normalizou com sucesso
                        titulo = f"🟢 **[JUCA SUPERVISOR] Auto-Recuperação Concluída com Sucesso!**"
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
                        "detalhe": f"Pipeline voltou ao status VERDE após {qtd_tentativas} tentativa(s) do Juca."
                    })
                    del incidentes[pid]
                    houve_alteracao = True
                relatorio.append({"id": pid, "nome": nome, "status": "VERDE", "motivo": motivo})
                continue

            # Caso 2: Em Execução
            if status == "EM_EXECUCAO":
                relatorio.append({"id": pid, "nome": nome, "status": "EM_EXECUCAO", "motivo": motivo})
                continue

            # Caso 3: Detecção de Falha ou Atraso (Amarelo ou Vermelho)
            if status in ["AMARELO_ATRASO", "VERMELHO_FALHA"]:
                if not incidente_ativo:
                    # Primeira detecção: Tentativa 1 de 4 de auto-recuperação
                    logger.info(f"[JUCA AUTO-HEAL] Iniciando auto-recuperação autônoma para {nome} (Tentativa 1/4)...")
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
                    # Incidente já em andamento: aguarda intervalo entre tentativas antes de disparar a próxima
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
                                # Já foram 4 tentativas que NÃO DERAM CERTO! Escalonamento Nível 2 (Chama Karl no Teams)
                                logger.warning(f"[JUCA ESCALONAMENTO] 4 tentativas falharam para {nome}! Chamando Karl no Teams...")
                                primeiro_dt = datetime.fromisoformat(incidente_ativo["primeiro_disparo"])
                                titulo = f"🚨 **[JUCA SUPERVISOR] Atenção Karl · 4 Tentativas Falharam!**"
                                msg = (
                                    f"Karl, preciso da sua entrada no circuito!\n\n"
                                    f"⚠️ **Problema:** A rotina **{nome}** apresentou falha ou atraso persistente ({motivo}).\n"
                                    f"🛠️ **Medida tomada pelo Juca:** Esgotei as **4 tentativas automáticas** de reexecução no GitHub Actions (iniciadas às {primeiro_dt.strftime('%H:%M:%S')}).\n"
                                    f"❌ **Resultado:** Todas as 4 tentativas não deram certo ou falharam. Minha auto-recuperação esgotou o limite de segurança!\n"
                                    f"👨‍💻 **Ação Requerida:** Por favor, verifique o repositório, credenciais ou conexões externas."
                                )
                                self.enviar_alerta_teams(titulo, msg, severity="danger", url_run=url_run)
                                incidente_ativo["escalonado_karl"] = True
                                incidente_ativo["status"] = "ESCALONADO"
                                historico.append({
                                    "data_hora": agora.strftime("%Y-%m-%d %H:%M:%S"),
                                    "pipeline": pid,
                                    "evento": "ESCALONADO_KARL",
                                    "detalhe": "Auto-recuperação tentada 4 vezes sem sucesso. Karl chamado no Teams."
                                })
                                houve_alteracao = True

                relatorio.append({"id": pid, "nome": nome, "status": f"PROBLEMA_{status}", "motivo": motivo})

        estado["incidentes"] = incidentes
        estado["historico_acoes"] = historico[-50:]
        estado["ultimo_check"] = agora.strftime("%Y-%m-%d %H:%M:%S")
        estado["status_geral"] = "ALERTA" if incidentes else "OPERACIONAL_VERDE"
        salvar_estado_juca(estado)

        return relatorio

    def responder_comando_teams(self, texto, user_name="Karl"):
        """
        Processa comandos interativos enviados ao Juca no Teams.
        Ex: 'Juca status', 'Juca atualiza b3', 'Juca ajuda'.
        """
        t = texto.upper()
        agora = datetime.now(BRT_TZ).strftime("%d/%m/%Y %H:%M:%S")

        # 1. Comandos de Atualização Manual ("Juca clica no botão")
        if any(w in t for w in ["ATUALIZA B3", "ATUALIZAR B3", "DISPARA B3", "RODA B3"]):
            sucesso, msg = self.disparar_workflow_botao("karl-albert/Atualizador_BigQuery_B3", "rotina_b3.yml")
            if sucesso:
                return (
                    f"⚡ **Comandante {user_name}! Ação Executada com Sucesso:**\n\n"
                    f"👉 Já cliquei no botão e acionei a atualização de **B3 Cotações & Ações** no GitHub Actions!\n"
                    f"🕒 Horário: {agora}\n"
                    f"🟡 Status: Em execução na nuvem. Estou monitorando o encerramento."
                )
            else:
                return f"❌ **Erro no Disparo:** Não consegui acionar o workflow B3: {msg}"

        if any(w in t for w in ["ATUALIZA MELI", "ATUALIZA MERCADO LIVRE", "RODA MELI"]):
            sucesso, msg = self.disparar_workflow_botao("karl-albert/Atualizador_BigQuery_MercadoLivre-", "rotina_mercadolivre.yml")
            if sucesso:
                return (
                    f"⚡ **Comandante {user_name}! Ação Executada com Sucesso:**\n\n"
                    f"👉 Já cliquei no botão e acionei a rotina do **Mercado Livre** no GitHub Actions!\n"
                    f"🕒 Horário: {agora}\n"
                    f"🟡 Status: Em execução na nuvem."
                )
            else:
                return f"❌ **Erro no Disparo:** Não consegui acionar o workflow Mercado Livre: {msg}"

        if any(w in t for w in ["ATUALIZA MACRO", "RODA MACRO"]):
            sucesso, msg = self.disparar_workflow_botao("karl-albert/Atualizador_BigQuery_B3", "rotina_macro.yml")
            if sucesso:
                return f"⚡ **Ação Executada:** Workflow **B3 Macroeconomia** disparado com sucesso às {agora}!"
            else:
                return f"❌ **Erro no Disparo:** {msg}"

        # 2. Relatório de Status Geral
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

        status_jocas = (
            "🤖 **Status dos 3 Jocas:**\n"
            "• **Joca_BigQuery (Telegram ML):** 🟢 Ativo na nuvem\n"
            "• **Joca_Fabric (Telegram OneLake):** 🟢 Ativo na nuvem\n"
            "• **Joca_B3 (Teams Mercado Financeiro):** 🟢 Ativo na nuvem"
        )

        linhas_txt = "\n".join(linhas)
        return (
            f"👑 **JUCA · PAINEL EXECUTIVO DE SUPERVISÃO**\n\n"
            f"Olá {user_name}! Sou o **Juca**, coordenador geral e supervisor dos 3 Jocas e de todos os pipelines.\n\n"
            f"{status_jocas}\n\n"
            f"📊 **Status dos Pipelines de Carga:**\n"
            f"{linhas_txt}\n\n"
            f"🛠️ **Auto-Recuperação Ativa:** Se qualquer rotina atrasar ou travar, eu mesmo clico no botão de atualizar. Se mesmo assim falhar, te chamo aqui imediatamente!\n"
            f"💡 *Para disparar manualmente agora, diga: `Juca atualiza b3` ou `Juca atualiza meli`.*"
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
