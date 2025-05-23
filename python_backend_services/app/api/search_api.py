# python_backend_services/app/api/search_api.py
# Simplified for Stage 1: Only the /search route calling the BM25-only orchestrator.
from flask import Blueprint, request, jsonify, current_app
import logging

# Adjust import path as necessary
try:
    from python_backend_services.app.services.search_orchestrator import SearchOrchestrator
except ImportError as e:
    logging.basicConfig(level="CRITICAL")
    logging.critical(f"CRITICAL: Failed to import SearchOrchestrator for API: {e}. API will not work.")
    raise

logger = logging.getLogger(__name__)
search_bp = Blueprint('search_api', __name__, url_prefix='/api/v1')  # Using a distinct blueprint name


def get_search_orchestrator_from_app() -> SearchOrchestrator:
    # Assumes orchestrator is initialized and stored in app.extensions by create_app
    orchestrator = current_app.extensions.get('search_orchestrator')
    if orchestrator is None:
        logger.critical("SearchOrchestrator not found in current_app.extensions.")
        raise RuntimeError("SearchOrchestrator service not initialized properly.")
    return orchestrator  # type: ignore


@search_bp.route('/search', methods=['POST'])
def search():
    try:
        orchestrator = get_search_orchestrator_from_app()
        if orchestrator.es_service is None or not hasattr(orchestrator.es_service,
                                                          'es_client') or orchestrator.es_service.es_client is None:
            logger.error("Search API: Elasticsearch service is not available in orchestrator.")
            return jsonify({"error": "Search service temporarily unavailable due to backend issue."}), 503
    except RuntimeError as e:
        logger.critical(f"Search API: {e}", exc_info=True)
        return jsonify({"error": "Search service failed to initialize or is not available."}), 500
    except Exception as e:
        logger.critical(f"Search API : Unexpected error getting SearchOrchestrator - {e}", exc_info=True)
        return jsonify({"error": "Search service failed to initialize."}), 500

    try:
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({"error": "Missing 'query' in request body"}), 400

        user_query = data['query']
        # Tags are optional for MVP, orchestrator might not use them in this simplified version
        # tags = data.get('tags')

        logger.info(f"API /search called with query: '{user_query}'")

        # Call the simplified BM25-only search method
        results = orchestrator.search_petitions_bm25_only(user_query)

        if results:
            # For MVP, we return a list of top N documents
            # Each item in the list has document_id, file_name, content_preview, score
            return jsonify(results), 200
        else:
            return jsonify({"message": "No documents found matching your query"}), 404

    except Exception as e:
        logger.error(f"Error in /search endpoint: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred"}), 500


# If you want a /document/{id} endpoint for Stage 1 (useful for debugging/verification):
@search_bp.route('/document/<string:document_id>', methods=['GET'])
def get_document(document_id):
    try:
        orchestrator = get_search_orchestrator_from_app()
        if orchestrator.es_service is None or not hasattr(orchestrator.es_service,
                                                          'es_client') or orchestrator.es_service.es_client is None:
            logger.error("Document API (Stage 1): Elasticsearch service is not available in orchestrator.")
            return jsonify({"error": "Document retrieval service temporarily unavailable."}), 503
    except RuntimeError as e:
        logger.critical(f"Document API : {e}", exc_info=True)
        return jsonify({"error": "Document retrieval service failed to initialize."}), 500
    except Exception as e:
        logger.critical(f"Document API : Unexpected error - {e}", exc_info=True)
        return jsonify({"error": "Document retrieval service failed."}), 500

    try:
        logger.info(f"API /document/{document_id} called")
        document_details = orchestrator.get_document_details_by_id(document_id)
        if document_details:
            return jsonify(document_details), 200
        else:
            return jsonify({"error": f"Document with ID '{document_id}' not found"}), 404
    except Exception as e:
        logger.error(f"Error in /document/{document_id} endpoint: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred"}), 500




