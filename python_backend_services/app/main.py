# python_backend_services/app/main.py
# Entry point for the Stage 1 MVP Flask application.
from flask import Flask, jsonify
import logging

# Adjust import paths as necessary
try:
    from python_backend_services.app.api.search_api import search_bp
    from python_backend_services.app.core.config import settings
    from python_backend_services.app.services.search_orchestrator import \
        SearchOrchestrator  # Use orchestrator
except ImportError as e:
    logging.basicConfig(level="CRITICAL", format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(message)s')
    logging.critical(
        f"CRITICAL: Failed to import necessary modules for Flask app : {e}. Application cannot start.")
    raise

# Configure logging AFTER settings are (presumably) imported successfully
logging.basicConfig(level=settings.LOG_LEVEL,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)


def create_app():
    app = Flask(__name__)
    app.config['JSON_AS_ASCII'] = False

    if not hasattr(app, 'extensions'):
        app.extensions = {}

    try:
        # Initialize the SearchOrchestrator
        app.extensions['search_orchestrator'] = SearchOrchestrator()
        logger.info("SearchOrchestrator initialized and attached to app.extensions.")
    except Exception as e:
        logger.critical(f"Failed to initialize SearchOrchestrator during app creation: {e}", exc_info=True)
        app.extensions['search_orchestrator'] = None  # Mark as unavailable

    # Register the blueprint
    app.register_blueprint(search_bp)
    logger.info("Flask application created and search_bp blueprint registered.")

    @app.route('/health', methods=['GET'])  # Distinct health check endpoint name
    def health_check():
        health_status = {"status": "healthy", "message": "Flask app is running."}

        orchestrator = app.extensions.get('search_orchestrator')
        if orchestrator and orchestrator.es_service and hasattr(orchestrator.es_service,
                                                                'es_client') and orchestrator.es_service.es_client:
            try:
                if orchestrator.es_service.es_client.ping():
                    health_status['elasticsearch_connection'] = 'ok'
                else:
                    health_status['elasticsearch_connection'] = 'error_ping_failed'
                    health_status['status'] = 'unhealthy'
            except Exception as es_err:
                health_status['elasticsearch_connection'] = f'error_exception_pinging: {str(es_err)}'
                health_status['status'] = 'unhealthy'
        elif orchestrator and (orchestrator.es_service is None or not hasattr(orchestrator.es_service,
                                                                              'es_client') or orchestrator.es_service.es_client is None):
            health_status['elasticsearch_connection'] = 'service_not_available_in_orchestrator'
            health_status['status'] = 'unhealthy'
        else:
            health_status['search_orchestrator'] = 'not_initialized_in_app_context'
            health_status['status'] = 'unhealthy'

        return jsonify(health_status), 200 if health_status['status'] == 'healthy' else 503

    return app


app = create_app()

if __name__ == '__main__':
    logger.info(f"Starting Flask development server (MVP) on [http://0.0.0.0:5000](http://0.0.0.0:5000)")
    # For Stage 1, debug mode is helpful.
    app.run(host='0.0.0.0', port=5000, debug=True)  # debug=True is fine for this stage