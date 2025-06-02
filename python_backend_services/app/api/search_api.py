# python_backend_services/app/api/search_api.py
from flask import Blueprint, request, jsonify, current_app
import logging

logger = logging.getLogger(__name__) # Logger específico para este módulo/blueprint

search_bp = Blueprint('search_bp', __name__)

@search_bp.route('/search', methods=['POST'])
def search_documents():
    """
    Endpoint para buscar documentos.
    Espera um JSON no corpo da requisição com a query do usuário.
    Ex: {"query": "petição de alimentos para menor"}
    """
    try:
        search_orchestrator = current_app.extensions.get('search_orchestrator')
        if not search_orchestrator:
            logger.error("SearchOrchestrator não encontrado na configuração da aplicação.")
            return jsonify({"error": "Serviço de busca não está disponível"}), 503

        data = request.get_json()
        if not data or "query" not in data:
            logger.warning("Requisição de busca recebida sem 'query' no corpo JSON.")
            return jsonify({"error": "A 'query' é obrigatória no corpo da requisição JSON"}), 400

        user_query = data["query"].strip()
        if not user_query:
            logger.warning("Requisição de busca recebida com 'query' vazia.")
            return jsonify({"error": "A 'query' não pode ser vazia"}), 400

        logger.info(f"Recebida requisição de busca: '{user_query}'")

        # Chamar o SearchOrchestrator
        # O resultado deve ser uma estrutura que o bot possa usar
        # (e.g., ID do documento escolhido, resumo contextual, conteúdo completo ou partes dele)
        search_result = search_orchestrator.search_and_rerank_documents(user_query)

        if search_result:
            # search_result é esperado ser um dicionário com 'chosen_document_id', 'contextual_summary', 'full_content', etc.
            # Conforme definido pelo retorno do SearchOrchestrator
            logger.info(f"Busca bem-sucedida para '{user_query}'. Documento escolhido ID: {search_result.get('chosen_document_id')}")
            return jsonify(search_result), 200
        else:
            # Pode acontecer se o orchestrator não encontrar nada ou se o LLM falhar no re-rank e não houver fallback
            logger.warning(f"Nenhum resultado encontrado ou falha no processamento para a query: '{user_query}'")
            return jsonify({"message": "Nenhum resultado adequado encontrado para sua consulta.", "results": []}), 404

    except Exception as e:
        logger.error(f"Erro inesperado durante a busca de documentos: {e}", exc_info=True)
        return jsonify({"error": "Ocorreu um erro interno ao processar sua solicitação de busca"}), 500

# Você pode adicionar outros endpoints aqui, como:
# @search_bp.route('/document/<string:doc_id>', methods=['GET'])
# def get_document_details(doc_id):
#     # Lógica para buscar e retornar detalhes de um documento específico pelo ID
#     # (pode buscar diretamente do Elasticsearch ou do SQLite)
#     search_orchestrator = current_app.extensions.get('search_orchestrator')
#     # Necessário importar settings se for usar aqui diretamente.
#     # from python_backend_services.app.core.config import settings
#     # E também elasticsearch exceptions
#     # import elasticsearch
#     if not search_orchestrator or not search_orchestrator.es_service:
#         return jsonify({"error": "Serviço não disponível"}), 503
#
#     try:
#         # Exemplo: buscar do Elasticsearch
#         document = search_orchestrator.es_service.es_client.get(
#             index=settings.ELASTICSEARCH_INDEX_NAME,
#             id=doc_id
#         )
#         return jsonify(document.get('_source', {})), 200
#     except elasticsearch.NotFoundError: # Supondo que 'elasticsearch' foi importado
#         return jsonify({"error": "Documento não encontrado"}), 404
#     except Exception as e:
#         logger.error(f"Erro ao buscar documento {doc_id}: {e}", exc_info=True)
#         return jsonify({"error": "Erro interno"}), 500