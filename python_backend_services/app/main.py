# python_backend_services/app/main.py
import logging
import os
import sys

# Importações de bibliotecas de terceiros devem vir primeiro
from flask import Flask, jsonify, current_app

# --- Adicionar o diretório raiz do projeto (bm25_mistral_pq) ao sys.path ---
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root_directory = os.path.abspath(os.path.join(current_script_dir, '..', '..'))

if project_root_directory not in sys.path:
    sys.path.insert(0, project_root_directory)
# --- Fim da adição ao sys.path ---

# Agora, as importações específicas do projeto
try:
    from python_backend_services.app.api.search_api import search_bp
    from python_backend_services.app.api.interaction_api import interaction_bp
    # Assumindo que você criará este arquivo para o webhook do WhatsApp:
    from python_backend_services.app.api.whatsapp_webhook_api import whatsapp_webhook_bp
    from python_backend_services.app.core.config import settings
    from python_backend_services.data_ingestion.indexer_service import \
        ElasticsearchService
    from python_backend_services.app.services.search_orchestrator import SearchOrchestrator
    from python_backend_services.app.services.llm_service import LLMService
    from python_backend_services.app.services.glossary_service import GlossaryService
except ImportError as e:
    temp_logger_main = logging.getLogger("FlaskMainAppInitError_Early")
    if not temp_logger_main.hasHandlers():
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
        handler.setFormatter(formatter)
        temp_logger_main.addHandler(handler)
        temp_logger_main.setLevel(logging.CRITICAL)

    sys_path_str = "\n".join(sys.path)
    temp_logger_main.critical(
        f"FALHA CRÍTICA (main.py): Erro ao importar módulos específicos do projeto: {e}. \n"
        f"Caminho do interpretador Python: {sys.executable}\n"
        f"Diretório de trabalho atual (CWD): {os.getcwd()}\n"
        f"Sys.path atual:\n{sys_path_str}\n"
        f"Project root calculado e adicionado ao sys.path: {project_root_directory}\n"
        "A aplicação Flask não pode iniciar. Verifique se o pacote 'python_backend_services' está no project_root_directory e se o CWD está correto.",
        exc_info=True)
    raise SystemExit(f"Erro de importação crítico em main.py: {e}")

# Configurar logging principal APÓS as importações terem sido bem-sucedidas
logging.basicConfig(level=settings.LOG_LEVEL.upper(),
                    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
                    force=True)
logger = logging.getLogger("FlaskMainApp")


def create_app(test_config=None):
    app = Flask(__name__)
    app.config['JSON_AS_ASCII'] = False
    app.config['PROPAGATE_EXCEPTIONS'] = True

    if test_config:
        app.config.from_mapping(test_config)
        logger.info("Aplicando configuração de teste.")

    try:
        logger.info("Inicializando serviços para a aplicação Flask...")

        glossary_service_instance = GlossaryService(glossary_tsv_path=settings.GLOSSARY_FILE_PATH)
        logger.info(
            f"GlossaryService inicializado. Termos carregados: {len(glossary_service_instance.glossary_data) if glossary_service_instance.glossary_data else '0 ou N/A'}")

        llm_service_instance = LLMService(glossary_service=glossary_service_instance)
        logger.info("LLMService inicializado.")

        elasticsearch_service_instance = ElasticsearchService(
            es_hosts=settings.ELASTICSEARCH_HOSTS,
            es_user=settings.ELASTICSEARCH_USER,
            es_password=settings.ELASTICSEARCH_PASSWORD,
            request_timeout=settings.ELASTICSEARCH_REQUEST_TIMEOUT
        )
        logger.info("ElasticsearchService inicializado.")

        try:
            logger.info(
                f"Verificando/criando índice principal '{settings.ELASTICSEARCH_INDEX_NAME}' no Elasticsearch...")
            elasticsearch_service_instance.create_index_if_not_exists(
                index_name=settings.ELASTICSEARCH_INDEX_NAME,
                embedding_dimensions=settings.EMBEDDING_DIMENSIONS
            )
        except Exception as e_idx_create:
            logger.error(
                f"Falha ao verificar/criar índice principal '{settings.ELASTICSEARCH_INDEX_NAME}' na inicialização: {e_idx_create}",
                exc_info=True)

        search_orchestrator_instance = SearchOrchestrator(
            es_service=elasticsearch_service_instance,
            llm_service=llm_service_instance,
            glossary_service=glossary_service_instance
        )
        logger.info("SearchOrchestrator inicializado.")

        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions['search_orchestrator'] = search_orchestrator_instance
        app.extensions['elasticsearch_service'] = elasticsearch_service_instance

        logger.info("Todos os serviços principais inicializados e SearchOrchestrator configurado na aplicação.")

    except Exception as e:
        logger.critical(f"FALHA CRÍTICA durante a inicialização dos serviços da aplicação: {e}", exc_info=True)
        raise RuntimeError(f"Falha ao inicializar serviços: {e}") from e

    # Registrar Blueprints
    app.register_blueprint(search_bp, url_prefix='/api/v1')
    app.register_blueprint(interaction_bp, url_prefix='/api/v1')
    app.register_blueprint(whatsapp_webhook_bp) # Registra o novo blueprint do WhatsApp

    logger.info(
        "Blueprints (search_bp, interaction_bp, whatsapp_webhook_bp) registrados. Aplicação Flask pronta para ser iniciada.") # Mensagem de log atualizada

    @app.route('/health', methods=['GET'])
    def health_check():
        status_code = 200
        health_info = {'status': 'healthy', 'services': {}}

        orchestrator = current_app.extensions.get('search_orchestrator')
        es_service_health = current_app.extensions.get('elasticsearch_service')

        if not orchestrator:
            health_info['status'] = 'unhealthy'
            health_info['search_orchestrator_status'] = 'NÃO inicializado'
            status_code = 503
        else:
            health_info['search_orchestrator_status'] = 'OK'

            if es_service_health and hasattr(es_service_health, 'es_client'):
                try:
                    if es_service_health.es_client.ping():
                        health_info['services']['elasticsearch'] = 'OK'
                    else:
                        health_info['services']['elasticsearch'] = 'UNAVAILABLE (ping failed)'
                        health_info['status'] = 'unhealthy'
                        status_code = 503
                except Exception as es_err:
                    health_info['services']['elasticsearch'] = f'ERROR ({type(es_err).__name__})'
                    health_info['status'] = 'unhealthy'
                    status_code = 503
            else:
                health_info['services']['elasticsearch'] = 'NÃO inicializado'
                health_info['status'] = 'unhealthy'
                status_code = 503

            if orchestrator.llm_service:
                health_info['services']['llm_service'] = 'OK (instanciado)'
            else:
                health_info['services']['llm_service'] = 'NÃO inicializado no orchestrator'
                health_info['status'] = 'unhealthy'
                status_code = 503

            if orchestrator.glossary_service:
                health_info['services'][
                    'glossary_service'] = f"OK (termos: {len(orchestrator.glossary_service.glossary_data) if orchestrator.glossary_service.glossary_data else '0'})"
            elif orchestrator.llm_service and orchestrator.llm_service.glossary_service:
                health_info['services'][
                    'glossary_service'] = f"OK (via LLMService, termos: {len(orchestrator.llm_service.glossary_service.glossary_data) if orchestrator.llm_service.glossary_service.glossary_data else '0'})"
            else:
                health_info['services']['glossary_service'] = 'NÃO inicializado'

        return jsonify(health_info), status_code

    return app


application = create_app()

if __name__ == '__main__':
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    debug_mode_str = os.getenv("FLASK_DEBUG", "False").lower()
    debug_mode = debug_mode_str in ['true', '1', 't', 'yes']

    logger.info(f"Iniciando servidor Flask de desenvolvimento em http://{host}:{port}/ (Debug: {debug_mode})")
    application.run(host=host, port=port, debug=debug_mode)