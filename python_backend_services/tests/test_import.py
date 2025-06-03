# test_import.py
print("Tentando importar LLMService...")
try:
    from python_backend_services.app.services.llm_service import LLMService
    print("LLMService importado com sucesso!")
    print(LLMService)
except ImportError as e:
    print(f"ImportError: {e}")
except Exception as e_gen:
    print(f"Outro erro durante o import: {e_gen}")

print("\nTentando importar settings...")
try:
    from python_backend_services.app.core.config import settings
    print("Settings importado com sucesso!")
    print(settings.OLLAMA_MODEL_NAME)
except ImportError as e:
    print(f"ImportError em settings: {e}")
except Exception as e_gen:
    print(f"Outro erro ao importar settings: {e_gen}")