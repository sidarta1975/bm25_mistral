# python_backend_services/app/main.py
import logging
import os
import sys

# Importações de bibliotecas de terceiros devem vir primeiro
from flask import Flask, jsonify, current_app
import gspread  # Adicionado para type hinting ou verificações, se necessário

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
    from python_backend_services.app.api.whatsapp_webhook_api import whatsapp_webhook_bp
    from python_backend_services.app.core.config import settings  # Agora contém as configs do GSheet
    from python_backend_services.data_ingestion.indexer_service import \
        ElasticsearchService
    from python_backend_services.app.services.search_orchestrator import SearchOrchestrator
    from python_backend_services.app.services.llm_service import LLMService
    # Assumindo que GlossaryService foi adaptado para GSheets ou você tem uma GlossaryServiceGS
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
# e settings estar disponível
logging.basicConfig(level=settings.LOG_LEVEL.upper(),
                    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
                    force=True)
logger = logging.getLogger("FlaskMainApp")


def create_app(test_config=None):
    app = Flask(__name__)
    app.config['JSON_AS_ASCII'] = False
    app.config['PROPAGATE_EXCEPTIONS'] = True

    # Aplicar configurações de 'settings' ou de 'test_config'
    active_settings = settings
    if test_config:
        # Se test_config for fornecido, sobrescrever/adicionar configurações.
        # Isso é mais complexo se 'settings' for um objeto e test_config um dict.
        # Para simplificar, se test_config for um objeto Settings mockado, pode ser usado diretamente.
        # Se for um dict, você precisaria de uma lógica para mesclar ou substituir.
        # Exemplo se test_config é um dict para sobrescrever o settings global (CUIDADO COM EFEITOS COLATERAIS):
        # for key, value in test_config.items():
        #     setattr(active_settings, key, value)
        # logger.info("Aplicando configuração de teste do dicionário.")
        # Ou se test_config é uma instância completa de Settings para teste:
        # active_settings = test_config
        # logger.info("Usando instância de Settings de teste fornecida.")
        app.config.from_mapping(test_config)  # Flask usa isso para suas configs internas
        logger.info("Aplicando configuração de teste ao Flask app.config.")

    try:
        logger.info("Inicializando serviços para a aplicação Flask...")

        # --- ATUALIZAÇÃO: Inicialização do GlossaryService com Google Sheets ---
        logger.info(f"Tentando inicializar GlossaryService com GSheet ID: {active_settings.GLOBAL_GLOSSARY_GSHEET_ID}")
        logger.info(f"Caminho da chave da conta de serviço: {active_settings.GCP_SERVICE_ACCOUNT_KEY_PATH}")

        glossary_service_instance = GlossaryService(
            service_account_path=active_settings.GCP_SERVICE_ACCOUNT_KEY_PATH,
            gsheet_id=active_settings.GLOBAL_GLOSSARY_GSHEET_ID,
            gsheet_tab_name=active_settings.GLOBAL_GLOSSARY_GSHEET_TAB_NAME
        )
        # --- FIM DA ATUALIZAÇÃO ---
        logger.info(
            f"GlossaryService inicializado. Termos carregados: {len(glossary_service_instance.glossary_data) if hasattr(glossary_service_instance, 'glossary_data') and glossary_service_instance.glossary_data else '0 ou N/A'}")

        llm_service_instance = LLMService(glossary_service=glossary_service_instance)
        logger.info("LLMService inicializado.")

        elasticsearch_service_instance = ElasticsearchService(
            es_hosts=active_settings.ELASTICSEARCH_HOSTS,  # Usar active_settings
            es_user=active_settings.ELASTICSEARCH_USER,
            es_password=active_settings.ELASTICSEARCH_PASSWORD,
            request_timeout=active_settings.ELASTICSEARCH_REQUEST_TIMEOUT
        )
        logger.info("ElasticsearchService inicializado.")

        try:
            logger.info(
                f"Verificando/criando índice principal '{active_settings.ELASTICSEARCH_INDEX_NAME}' no Elasticsearch...")
            elasticsearch_service_instance.create_index_if_not_exists(
                index_name=active_settings.ELASTICSEARCH_INDEX_NAME,
                embedding_dimensions=active_settings.EMBEDDING_DIMENSIONS
            )
        except Exception as e_idx_create:
            logger.error(
                f"Falha ao verificar/criar índice principal '{active_settings.ELASTICSEARCH_INDEX_NAME}' na inicialização: {e_idx_create}",
                exc_info=True)
            # Considerar se deve levantar o erro aqui ou permitir que a app continue com o ES potencialmente indisponível

        search_orchestrator_instance = SearchOrchestrator(
            es_service=elasticsearch_service_instance,
            llm_service=llm_service_instance,
            glossary_service=glossary_service_instance
            # Se SearchOrchestrator precisar de config, passe active_settings
        )
        logger.info("SearchOrchestrator inicializado.")

        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions['search_orchestrator'] = search_orchestrator_instance
        app.extensions['elasticsearch_service'] = elasticsearch_service_instance
        # Guardar também o glossary_service e llm_service se forem acessados diretamente por rotas
        app.extensions['glossary_service'] = glossary_service_instance
        app.extensions['llm_service'] = llm_service_instance

        logger.info("Todos os serviços principais inicializados e configurados na aplicação.")

    except FileNotFoundError as fnf_error:
        # Especificamente para o arquivo de credenciais GCP
        if hasattr(active_settings, 'GCP_SERVICE_ACCOUNT_KEY_PATH') and str(fnf_error).count(
                active_settings.GCP_SERVICE_ACCOUNT_KEY_PATH):
            logger.critical(
                f"FALHA CRÍTICA: Arquivo de credenciais da conta de serviço GCP não encontrado em '{active_settings.GCP_SERVICE_ACCOUNT_KEY_PATH}'. {fnf_error}",
                exc_info=True)
            raise RuntimeError(f"Arquivo de credenciais GCP não encontrado: {fnf_error}") from fnf_error
        else:
            logger.critical(f"FALHA CRÍTICA: Arquivo não encontrado durante a inicialização dos serviços: {fnf_error}",
                            exc_info=True)
            raise RuntimeError(f"Arquivo não encontrado: {fnf_error}") from fnf_error
    except gspread.exceptions.GSpreadException as gspread_error:
        # Captura erros específicos do gspread (SpreadsheetNotFound, WorksheetNotFound, APIError etc.)
        logger.critical(
            f"FALHA CRÍTICA: Erro ao acessar Google Sheets durante a inicialização dos serviços: {gspread_error}",
            exc_info=True)
        raise RuntimeError(f"Erro Google Sheets: {gspread_error}") from gspread_error
    except Exception as e:
        logger.critical(f"FALHA CRÍTICA durante a inicialização dos serviços da aplicação: {type(e).__name__} - {e}",
                        exc_info=True)
        # Não relance o erro genérico 'e' diretamente se você já logou os detalhes,
        # para não perder o traceback original nas mensagens de erro subsequentes do Flask/Pytest.
        # O 'from e' no raise já preserva a causa.
        raise RuntimeError(f"Falha ao inicializar serviços: {type(e).__name__} - {e}") from e

    # Registrar Blueprints
    app.register_blueprint(search_bp, url_prefix='/api/v1')
    app.register_blueprint(interaction_bp, url_prefix='/api/v1')
    app.register_blueprint(whatsapp_webhook_bp)

    logger.info(
        "Blueprints (search_bp, interaction_bp, whatsapp_webhook_bp) registrados. Aplicação Flask pronta para ser iniciada.")

    @app.route('/health', methods=['GET'])
    def health_check():
        status_code = 200
        # Usar active_settings aqui também, se o 'settings' global puder ser diferente do usado na criação.
        # No entanto, current_app.extensions já deve ter as instâncias corretas.
        health_info = {'status': 'healthy', 'services': {}}

        orchestrator = current_app.extensions.get('search_orchestrator')
        es_service_health = current_app.extensions.get('elasticsearch_service')
        glossary_service_health = current_app.extensions.get('glossary_service')  # Pegar a instância guardada
        llm_service_health = current_app.extensions.get('llm_service')  # Pegar a instância guardada

        # Search Orchestrator
        if not orchestrator:
            health_info['status'] = 'unhealthy'
            health_info['search_orchestrator_status'] = 'NÃO inicializado'
            status_code = 503
        else:
            health_info['search_orchestrator_status'] = 'OK'

        # Elasticsearch Service
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
            if status_code == 200: status_code = 503  # Se ainda não era unhealthy
            health_info['status'] = 'unhealthy'

        # LLM Service
        if llm_service_health:
            health_info['services']['llm_service'] = 'OK (instanciado)'
        else:
            health_info['services']['llm_service'] = 'NÃO inicializado'
            if status_code == 200: status_code = 503
            health_info['status'] = 'unhealthy'

        # Glossary Service
        if glossary_service_health and hasattr(glossary_service_health, 'glossary_data'):
            health_info['services'][
                'glossary_service'] = f"OK (termos: {len(glossary_service_health.glossary_data) if glossary_service_health.glossary_data is not None else '0'})"
        else:
            health_info['services']['glossary_service'] = 'NÃO inicializado ou sem dados'
            # Não necessariamente um erro crítico para o health status geral, a menos que seja mandatório.
            # if status_code == 200: status_code = 503
            # health_info['status'] = 'unhealthy'

        return jsonify(health_info), status_code

    return app


# Esta parte é executada quando o script é chamado diretamente (ex: python main.py ou flask run)
# Os testes Pytest geralmente importam create_app e a executam, então eles também passam por aqui.
application = create_app()

if __name__ == '__main__':
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    debug_mode_str = os.getenv("FLASK_DEBUG", "False").lower()
    debug_mode = debug_mode_str in ['true', '1', 't', 'yes']

    logger.info(f"Iniciando servidor Flask de desenvolvimento em http://{host}:{port}/ (Debug: {debug_mode})")
    application.run(host=host, port=port, debug=debug_mode)