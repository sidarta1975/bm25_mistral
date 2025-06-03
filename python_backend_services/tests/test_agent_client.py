import requests
import json

SEARCH_API_URL = "http://127.0.0.1:5000/api/v1/search"  # Ajuste se seu host/porta for diferente


def send_search_query(query_text):
    """Envia uma query de busca para a API e imprime a resposta."""
    if not query_text.strip():
        print("Query não pode ser vazia. Tente novamente.")
        return

    payload = {"query": query_text}
    headers = {"Content-Type": "application/json"}

    print(f"\nEnviando query: \"{query_text}\" para {SEARCH_API_URL}")

    try:
        response = requests.post(SEARCH_API_URL, headers=headers, json=payload)
        print(f"Status Code: {response.status_code}")

        try:
            response_json = response.json()
            print("Response JSON:")
            # Imprime o JSON de forma legível
            print(json.dumps(response_json, indent=2, ensure_ascii=False))
        except json.JSONDecodeError:
            # Se a resposta não for JSON, imprime o texto bruto
            print("Response (Not JSON):")
            print(response.text)

    except requests.exceptions.ConnectionError as e:
        print(f"Erro de conexão: Não foi possível conectar a {SEARCH_API_URL}")
        print(f"Verifique se a aplicação Flask (python python_backend_services/app/main.py) está em execução.")
        print(f"Detalhes: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
    finally:
        print("\n" + "=" * 70)


if __name__ == "__main__":
    print("Cliente de Teste Interativo para a API de Busca")
    print("Digite sua consulta ou 'sair'/'exit' para terminar.")
    print("=" * 70)

    while True:
        try:
            user_input = input("Sua query: ")
        except KeyboardInterrupt:  # Permite sair com Ctrl+C
            print("\nSaindo...")
            break

        if user_input.lower() in ["sair", "exit"]:
            print("Saindo...")
            break

        send_search_query(user_input)