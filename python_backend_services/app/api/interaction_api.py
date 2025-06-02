# python_backend_services/app/api/interaction_api.py
from flask import Blueprint, request, jsonify, current_app
import logging
import sqlite3
from datetime import datetime  # Para validar timestamps, se necessário

# Importar settings para obter o caminho do BD, se não for passado de outra forma
try:
    from python_backend_services.app.core.config import settings
except ImportError:
    # Fallback mínimo se settings não puder ser importado (improvável com 'python -m')
    class MockSettingsInteraction:
        SQLITE_DB_PATH = "fallback_enriched_documents.sqlite"  # Não ideal


    settings = MockSettingsInteraction()

logger = logging.getLogger(__name__)  # Logger específico para este módulo/blueprint

interaction_bp = Blueprint('interaction_bp', __name__)


def get_db_connection_for_interaction_api():
    """Estabelece conexão com o banco de dados SQLite para esta API."""
    # Nota: Em uma aplicação Flask maior, você poderia gerenciar a conexão DB
    # de forma mais centralizada (e.g., usando Flask-SQLAlchemy ou um g.db no contexto da app).
    # Por simplicidade aqui, conectamos diretamente.
    try:
        conn = sqlite3.connect(settings.SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row  # Acesso às colunas por nome
        return conn
    except sqlite3.Error as e:
        logger.error(f"Erro ao conectar ao banco de dados SQLite em {settings.SQLITE_DB_PATH} a partir da API: {e}",
                     exc_info=True)
        raise  # Re-levanta a exceção para ser tratada pelo endpoint


@interaction_bp.route('/log_interaction', methods=['POST'])
def log_interaction():
    """
    Endpoint para receber e salvar logs de interação do bot do WhatsApp.
    Espera um JSON no corpo da requisição com os dados do log.
    Ex: { "user_phone": "...", "interaction_id": "...", "user_message": "...",
           "agent_response": "...", "document_id_sent": "...",
           "feedback_raw": "...", "feedback_sentiment": "..." }
    """
    log_data = request.get_json()

    if not log_data:
        logger.warning("Recebido pedido de log de interação sem corpo JSON.")
        return jsonify({"error": "Corpo da requisição JSON ausente"}), 400

    logger.debug(f"Recebido log de interação para salvar: {log_data}")

    # Campos esperados (alguns podem ser opcionais dependendo da etapa da conversa)
    required_fields = ["user_phone", "interaction_id"]  # Mínimo para um log útil
    optional_fields = ["user_message", "agent_response", "document_id_sent",
                       "feedback_raw", "feedback_sentiment", "error_message"]

    for field in required_fields:
        if field not in log_data or not log_data[field]:
            logger.warning(f"Campo obrigatório '{field}' ausente ou vazio no log de interação.")
            return jsonify({"error": f"Campo obrigatório '{field}' ausente ou vazio"}), 400

    conn = None
    try:
        conn = get_db_connection_for_interaction_api()
        cursor = conn.cursor()

        # Prepara os dados para inserção, usando None para campos opcionais ausentes
        insert_data = {
            "timestamp": datetime.now().isoformat(),  # Timestamp gerado no servidor
            "user_phone": log_data["user_phone"],
            "interaction_id": log_data["interaction_id"],
            "user_message": log_data.get("user_message"),
            "agent_response": log_data.get("agent_response"),
            "document_id_sent": log_data.get("document_id_sent"),
            "feedback_raw": log_data.get("feedback_raw"),
            "feedback_sentiment": log_data.get("feedback_sentiment"),
            "error_message": log_data.get("error_message")
        }

        columns = ', '.join(insert_data.keys())
        placeholders = ', '.join(['?'] * len(insert_data))
        sql = f"INSERT INTO interaction_logs ({columns}) VALUES ({placeholders})"

        cursor.execute(sql, list(insert_data.values()))
        conn.commit()

        log_id = cursor.lastrowid
        logger.info(f"Log de interação salvo com ID: {log_id} para interaction_id: {log_data['interaction_id']}")

        # --- Processamento de Feedback (Início da Fase 3.A) ---
        if insert_data["feedback_sentiment"] == "positive" and insert_data["document_id_sent"]:
            doc_id = insert_data["document_id_sent"]
            # A query original do usuário que levou a esta petição aprovada
            # precisaria ser recuperada ou passada junto com o log de feedback.
            # Assumindo que `user_message` no log de feedback contenha a query original
            # ou que o bot envie a `last_query` junto com o feedback.
            # Para simplificar, vamos assumir que a query original não está diretamente no log de feedback,
            # mas o bot poderia enviar `last_query` do seu estado.
            # Por agora, vamos focar em contar a aprovação para o document_id.
            # A query_hash viria depois.

            # Simulação de normalized_query_hash (a ser melhorado na Fase 3)
            # Se o bot enviar a query original que gerou este feedback, use-a aqui.
            # Por ora, usaremos um placeholder se não vier.
            original_user_query_for_feedback = log_data.get("original_user_query", "unknown_query_for_feedback")
            normalized_query_hash = original_user_query_for_feedback.lower().replace(" ", "_")  # Simplista

            cursor.execute("""
                           INSERT INTO approved_petition_stats (document_id, normalized_query_hash, approval_count, last_approved_at)
                           VALUES (?, ?, 1, CURRENT_TIMESTAMP) ON CONFLICT(document_id, normalized_query_hash) DO
                           UPDATE SET
                               approval_count = approval_count + 1,
                               last_approved_at = CURRENT_TIMESTAMP
                           """, (doc_id, normalized_query_hash))
            conn.commit()
            logger.info(
                f"Feedback positivo para document_id '{doc_id}' (query: '{original_user_query_for_feedback}') registrado/atualizado em approved_petition_stats.")

        return jsonify({"message": "Log de interação recebido com sucesso", "log_id": log_id}), 201

    except sqlite3.Error as e_sql:
        logger.error(f"Erro SQLite ao salvar log de interação: {e_sql}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": "Erro no banco de dados ao salvar log"}), 500
    except Exception as e:
        logger.error(f"Erro inesperado ao processar log de interação: {e}", exc_info=True)
        return jsonify({"error": "Erro interno do servidor ao processar log"}), 500
    finally:
        if conn:
            conn.close()