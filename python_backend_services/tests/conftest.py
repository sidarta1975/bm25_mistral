# python_backend_services/tests/conftest.py
# THIS FILE MUST BE NAMED 'conftest.py'
import pytest
from unittest.mock import MagicMock, patch

# Import the create_app function from your Stage 1 main application file
try:
    from python_backend_services.app.main import create_app
    from python_backend_services.app.core.config import settings
    # Import SearchOrchestrator to be patched during app creation for API tests
    from python_backend_services.app.services.search_orchestrator import SearchOrchestrator
except ImportError as e:
    print(
        f"CONFT_ERROR: Failed to import app components for testing: {e}. Check PYTHONPATH and __init__.py files.")


    # Fallback definitions
    def create_app():
        print("CONFT_WARNING: Using dummy create_app due to import error.")
        mock_app = MagicMock(name="DummyFlaskTestApp")
        mock_app.extensions = {}
        mock_app.test_client = MagicMock(return_value=MagicMock(name="DummyTestClient"))
        mock_app.config = {}

        def update_config(cfg_dict): mock_app.config.update(cfg_dict)

        mock_app.config.update = update_config  # type: ignore
        return mock_app


    class DummySettingsConftest:
        LOG_LEVEL = "DEBUG";
        ELASTICSEARCH_INDEX_NAME = "test_dummy_index_conftest";
        ELASTICSEARCH_HOSTS = ["http://dummy-es-conftest"];
        ELASTICSEARCH_USER = None;
        ELASTICSEARCH_PASSWORD = None;
        BM25_TOP_N_RESULTS = 5


    settings = DummySettingsConftest()


    class SearchOrchestrator:
        def __init__(self):
            self.es_service = MagicMock()
            self.es_service.es_client = MagicMock()
            self.es_service.es_client.ping.return_value = True


@pytest.fixture(scope='session')
def app():
    """
    Creates the Flask app for tests.
    Patches SearchOrchestrator globally for the app context before app creation
    to prevent real ES connection attempts during app initialization for API tests.
    """
    # The 'name' parameter sets the _mock_name internal attribute and is used in repr()
    mock_orchestrator_for_app_init = MagicMock(spec=SearchOrchestrator, name="AppInitMockOrchestrator")
    mock_orchestrator_for_app_init.es_service = MagicMock(name="AppInitMockESService")
    mock_orchestrator_for_app_init.es_service.es_client = MagicMock(name="AppInitMockESClient")
    mock_orchestrator_for_app_init.es_service.es_client.ping.return_value = True

    with patch('python_backend_services.app.main.SearchOrchestrator',
               return_value=mock_orchestrator_for_app_init):
        flask_app = create_app()

    flask_app.config.update({"TESTING": True})
    yield flask_app


@pytest.fixture()
def client(app):
    """A test client for the Stage 1 app. Depends on app_stage1."""
    return app.test_client()


@pytest.fixture
def mock_app_settings(monkeypatch):
    """Mocks application settings for tests (mostly for service-level unit tests)."""
    monkeypatch.setattr(settings, 'ELASTICSEARCH_HOSTS', ['http://mock-elasticsearch:9200'], raising=False)
    monkeypatch.setattr(settings, 'ELASTICSEARCH_INDEX_NAME', 'mock_test_index_mvp', raising=False)
    monkeypatch.setattr(settings, 'GLOSSARY_FILE_PATH', 'mock_glossary_stage1.tsv', raising=False)
    monkeypatch.setattr(settings, 'BM25_TOP_N_RESULTS', 3, raising=False)
    return settings

