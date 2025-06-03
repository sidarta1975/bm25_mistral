# python_backend_services/app/api/whatsapp_webhook_api.py
from flask import Blueprint, request, jsonify, current_app
import logging
import requests  # Para fazer requisições HTTP para o serviço Baileys
import time  # Para gerar IDs de interação únicos e para pausas
import os  # Para manipulação de caminhos, se necessário
import sys  # Para manipulação de caminhos, se necessário
import hashlib  # Para anonimização, se decidir usar
from typing import Dict, Any, Optional, List, Union  # <--- ADICIONADA IMPORTAÇÃO DE Dict, Any, Optional, List, Union

# ----- Início da Definição da Classe AdvogadaParceiraAgent -----
# Mova esta classe para seu próprio arquivo (ex: python_backend_services/app/agents/advogada_wa.py)
# e importe-a. Por exemplo:
# from python_backend_services.app.agents.advogada_wa import AdvogadaParceiraAgent
# Se você já fez isso, remova a definição da classe daqui e use a importação.

logger_agent = logging.getLogger("AdvogadaParceiraAgent")


class AdvogadaParceiraAgent:
    def __init__(self, agent_key: str = ""):
        self.agent_key = agent_key
        self.state = "waiting_activation"
        self.user_query_for_search: Optional[str] = None
        self.last_doc_id_sent: Optional[str] = None
        logger_agent.debug(f"[{self.agent_key}] Nova instância criada, estado inicial: {self.state}")

    def reset(self):
        self.state = "waiting_activation"
        self.user_query_for_search = None
        self.last_doc_id_sent = None
        logger_agent.debug(f"[{self.agent_key}] Agente resetado para o estado: {self.state}")

    def handle_message(self, message_text_original: str) -> Optional[Dict[str, Any]]:
        message_text_normalized = message_text_original.strip().lower()
        logger_agent.debug(
            f"[{self.agent_key}] handle_message - Estado atual: {self.state}, Msg recebida: '{message_text_original}' (Normalizada: '{message_text_normalized}')")

        if self.state == "waiting_activation":
            if "advogada parceira" in message_text_normalized:
                self.state = "awaiting_request"
                logger_agent.info(f"[{self.agent_key}] Frase de ativação recebida. Estado -> {self.state}")
                return {"type": "text",
                        "content": "👩‍⚖️ Oi, tudo bem? Eu sou a *Advogada Parceira*, sua assistente para elaboração de petições.\n\nPode me dizer o que você precisa? ✍️ Pode ser um pedido direto de petição (ex: 'quero uma ação de guarda') ou uma descrição do caso."}
            else:
                logger_agent.debug(
                    f"[{self.agent_key}] No estado '{self.state}', mensagem não é de ativação. Ignorando.")
                return None

        elif self.state == "awaiting_request":
            self.user_query_for_search = message_text_original
            self.state = "performing_search"
            logger_agent.info(
                f"[{self.agent_key}] Query recebida para busca: '{self.user_query_for_search}'. Estado -> {self.state}")
            return {"type": "search", "query": self.user_query_for_search}

        elif self.state == "awaiting_approval":
            original_query_for_log = self.user_query_for_search or "N/A (query anterior não registrada)"
            doc_id_for_log = self.last_doc_id_sent

            if message_text_normalized == "1" or "sim" in message_text_normalized:
                logger_agent.info(f"[{self.agent_key}] Feedback positivo recebido.")
                response_msg = "🙌 Que ótimo! Fico feliz em ajudar.\n\nSe precisar de outra petição, é só digitar **'advogada parceira'** novamente. Estou à disposição!"
                # Salva o doc_id e query antes de resetar
                current_doc_id = self.last_doc_id_sent
                current_original_query = self.user_query_for_search
                self.reset()
                return {"type": "text_and_log_feedback", "content": response_msg, "feedback": "positive",
                        "doc_id": current_doc_id, "original_query": current_original_query}
            elif message_text_normalized == "2" or "não" in message_text_normalized:
                self.state = "offer_alternative"
                logger_agent.info(f"[{self.agent_key}] Feedback negativo recebido. Estado -> {self.state}")
                return {"type": "text",
                        "content": "Tudo bem, obrigada pelo retorno!\n\nDeseja que eu apresente **outra petição** com base no seu pedido original?\nSe sim, digite **1**.\nSe não, digite **2**."}
            else:
                logger_agent.debug(
                    f"[{self.agent_key}] Feedback inválido no estado '{self.state}'. Solicitando correção.")
                return {"type": "text",
                        "content": "Por favor, responda com 'sim' (ou 1) para aprovar ou 'não' (ou 2) para não aprovar."}

        elif self.state == "offer_alternative":
            original_query_for_log = self.user_query_for_search or "N/A (query anterior não registrada)"
            doc_id_for_log = self.last_doc_id_sent

            if message_text_normalized == "1" or "sim" in message_text_normalized:
                self.state = "performing_search"
                logger_agent.info(
                    f"[{self.agent_key}] Usuário pediu alternativa. Estado -> {self.state} para query: '{original_query_for_log}'")
                return {"type": "search", "query": original_query_for_log, "alternative_search": True}
            elif message_text_normalized == "2" or "não" in message_text_normalized:
                logger_agent.info(f"[{self.agent_key}] Usuário não quis alternativa.")
                response_msg = "Entendido.\n\nQuando quiser elaborar uma nova petição, é só digitar **'advogada parceira'**. Estarei aqui para te ajudar. Até logo! 👋"
                # Salva o doc_id e query antes de resetar
                current_doc_id = self.last_doc_id_sent
                current_original_query = self.user_query_for_search
                self.reset()
                return {"type": "text_and_log_feedback", "content": response_msg,
                        "feedback": "negative_declined_alternative", "doc_id": current_doc_id,
                        "original_query": current_original_query}
            else:
                logger_agent.debug(
                    f"[{self.agent_key}] Resposta inválida no estado '{self.state}'. Solicitando correção.")
                return {"type": "text",
                        "content": "Por favor, responda com **1** para ver outra petição ou **2** para encerrar."}

        logger_agent.warning(
            f"[{self.agent_key}] Estado não tratado ou fluxo inesperado. Estado: {self.state}, Mensagem: '{message_text_normalized}'")
        return None


# ----- Fim da Definição da Classe AdvogadaParceiraAgent -----


logger = logging.getLogger(__name__)
whatsapp_webhook_bp = Blueprint('whatsapp_webhook_bp', __name__)

active_conversations: Dict[str, AdvogadaParceiraAgent] = {}

BAILEYS_SEND_API_URL = "http://localhost:3000/send-message"
INTERACTION_LOG_API_URL = "http://localhost:5000/api/v1/log_interaction"


def get_or_create_agent_instance(conversation_key: str) -> AdvogadaParceiraAgent:
    if conversation_key not in active_conversations:
        active_conversations[conversation_key] = AdvogadaParceiraAgent(agent_key=conversation_key)
        logger.info(
            f"Nova instância de AdvogadaParceiraAgent CRIADA para {conversation_key}. Estado: {active_conversations[conversation_key].state}")
    return active_conversations[conversation_key]


def send_message_to_baileys_service(recipient_id: str, text_message: str):
    if not text_message:
        logger.warning(f"Tentativa de enviar mensagem vazia para {recipient_id}. Ignorando.")
        return False

    payload = {"to": recipient_id, "message": text_message}
    logger.debug(
        f"Tentando enviar para Baileys API ({BAILEYS_SEND_API_URL}) -> Para: {recipient_id}, Msg: '{text_message[:100]}...'")
    try:
        response = requests.post(BAILEYS_SEND_API_URL, json=payload, timeout=15)
        response.raise_for_status()
        logger.info(f"Mensagem enviada para {recipient_id} via Baileys Service. Resposta: {response.json()}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Falha ao enviar mensagem para {recipient_id} via Baileys Service: {e}")
        return False


def log_interaction_to_api(log_payload: Dict[str, Any]):
    logger.debug(f"Tentando logar interação via API: {log_payload}")
    try:
        response = requests.post(INTERACTION_LOG_API_URL, json=log_payload, timeout=10)
        response.raise_for_status()
        logger.info(
            f"Feedback logado para ID Anonimizado {log_payload.get('anonymized_sender_id')} (Interaction ID: {log_payload.get('interaction_id')}): {response.json()}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao logar feedback para ID Anonimizado {log_payload.get('anonymized_sender_id')}: {e}")


@whatsapp_webhook_bp.route('/whatsapp-webhook', methods=['POST'])
def handle_whatsapp_message():
    data = request.get_json()
    if not data:
        logger.warning("Webhook do WhatsApp: Payload JSON ausente.")
        return jsonify({"status": "error", "message": "Payload JSON ausente"}), 400

    group_id = data.get('groupId')
    sender_id_original = data.get('senderId')
    message_text_original = data.get('message')

    if not group_id or not sender_id_original or message_text_original is None:
        logger.warning(f"Webhook do WhatsApp: Dados ausentes. Recebido: {data}")
        return jsonify({"status": "error", "message": "groupId, senderId ou message ausentes"}), 400

    logger.info(f"Webhook de {sender_id_original} em {group_id}: '{message_text_original}'")

    if group_id.endswith('@g.us'):
        conversation_key = f"{group_id}_{sender_id_original}"
    else:
        conversation_key = sender_id_original

    agent_instance = get_or_create_agent_instance(conversation_key)

    if "advogada parceira" in message_text_original.strip().lower():
        logger.info(
            f"Frase de ativação 'advogada parceira' detectada para {conversation_key}. Resetando o estado do agente.")
        agent_instance.reset()

    logger.info(f"Estado do agente para '{conversation_key}' ANTES de handle_message: {agent_instance.state}")
    agent_action = agent_instance.handle_message(message_text_original)
    logger.info(f"Ação retornada pelo agente para '{conversation_key}': {agent_action}")

    text_to_send_part1 = None
    text_to_send_part2 = None

    if agent_action:
        action_type = agent_action.get("type")
        logger.info(f"Tipo de ação para '{conversation_key}': {action_type}")

        if action_type == "text":
            text_to_send_part1 = agent_action.get("content")

        elif action_type == "search":
            user_query_for_search = agent_action.get("query")
            logger.info(f"Iniciando busca para: '{user_query_for_search}' (key: {conversation_key})")
            search_orchestrator = current_app.extensions.get('search_orchestrator')

            if search_orchestrator:
                search_result = search_orchestrator.search_and_rerank_documents(user_query_for_search)
                if search_result and search_result.get("full_text_content") and search_result.get("chosen_document_id"):
                    text_to_send_part1 = search_result["full_text_content"]
                    if agent_instance:
                        agent_instance.last_doc_id_sent = search_result["chosen_document_id"]
                        agent_instance.user_query_for_search = user_query_for_search  # Garante que a query original seja mantida para o feedback
                        agent_instance.state = "awaiting_approval"
                    text_to_send_part2 = "Esta petição te atende? Digite 'sim' ou 'não'."
                    logger.info(f"Estado do agente para '{conversation_key}' mudou para: awaiting_approval após busca.")
                elif search_result and search_result.get("error"):
                    text_to_send_part1 = f"Desculpe, ocorreu um erro durante a busca: {search_result['error']}"
                    if agent_instance: agent_instance.reset()
                else:
                    text_to_send_part1 = "Desculpe, não consegui encontrar uma petição para sua solicitação. Pode tentar outros termos?"
                    if agent_instance: agent_instance.state = "awaiting_request"
            else:
                text_to_send_part1 = "Serviço de busca indisponível no momento."
                if agent_instance: agent_instance.reset()

        elif action_type == "text_and_log_feedback":
            text_to_send_part1 = agent_action.get("content")

            interaction_id_for_log = f"{conversation_key}_{int(time.time())}"
            anonymized_sender_for_log = hashlib.sha256(sender_id_original.encode('utf-8')).hexdigest()[:16]

            log_payload = {
                "interaction_id": interaction_id_for_log,
                "group_id": group_id if group_id.endswith('@g.us') else None,
                "anonymized_sender_id": anonymized_sender_for_log,
                "user_request_text": agent_action.get("original_query", "N/A"),
                "agent_response_summary": f"Petição ID {agent_action.get('doc_id')}",
                "sent_document_id": agent_action.get("doc_id"),
                "user_feedback_raw": message_text_original,
                "feedback_sentiment": agent_action.get("feedback"),
                "original_user_query": agent_action.get("original_query", "N/A")
            }
            log_interaction_to_api(log_payload)

    if text_to_send_part1:
        send_message_to_baileys_service(group_id, text_to_send_part1)

    if text_to_send_part2:
        time.sleep(1)
        send_message_to_baileys_service(group_id, text_to_send_part2)

    return jsonify({"status": "webhook_processed"}), 200