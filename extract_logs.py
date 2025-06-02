import sqlite3
import json  # Para exportar para JSON
import csv  # Para exportar para CSV

# Caminho para o seu banco de dados SQLite
# Certifique-se de que este caminho esteja correto ou pegue de config.py
DB_PATH = "/python_backend_services/shared_data/enriched_documents_antigo.sqlite"

# Colunas que você quer extrair por padrão
DEFAULT_COLUMNS_TO_EXTRACT = [
    "log_id",
    "timestamp",
    "user_message",
    "agent_response",
    "document_id_sent",
    "feedback_raw",
    "feedback_sentiment",
    "error_message"
]


def fetch_interaction_logs(db_path, columns, start_datetime=None, end_datetime=None):
    """
    Busca logs de interação do banco de dados SQLite, opcionalmente filtrando por um intervalo de datas.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Acesso às colunas por nome
        cursor = conn.cursor()

        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM interaction_logs"

        conditions = []
        params = []

        if start_datetime:
            conditions.append("timestamp >= ?")
            params.append(start_datetime)

        if end_datetime:
            conditions.append("timestamp <= ?")
            params.append(end_datetime)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY timestamp DESC;"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        logs = [dict(row) for row in rows]
        return logs

    except sqlite3.Error as e:
        print(f"Erro ao conectar ou buscar no SQLite: {e}")
        return []
    finally:
        if conn:
            conn.close()


def save_to_json(data, filename="interaction_logs.json"):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Dados salvos em {filename}")


def save_to_csv(data, filename="interaction_logs.csv", columns=None):
    if not data:
        print("Nenhum dado para salvar em CSV.")
        return

    if columns is None:  # Se as colunas não foram especificadas, pega do primeiro registro
        if data:  # Garante que há dados para obter as chaves
            columns = data[0].keys()
        else:
            print("Não foi possível determinar colunas para CSV, pois não há dados.")
            return

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)
    print(f"Dados salvos em {filename}")


if __name__ == "__main__":
    # Para usar as configurações do projeto (recomendado para obter DB_PATH)
    # try:
    #     from python_backend_services.app.core.config import settings
    #     db_path_from_config = settings.SQLITE_DB_PATH
    # except ImportError:
    #     print("Não foi possível importar 'settings'. Usando DB_PATH definido no script.")
    #     db_path_from_config = DB_PATH

    # Para este exemplo, vamos continuar usando o DB_PATH definido no script.
    # Se você descomentar o bloco acima, substitua DB_PATH por db_path_from_config nas chamadas.

    print("--- Buscando todos os logs de interação ---")
    all_logs_data = fetch_interaction_logs(DB_PATH, DEFAULT_COLUMNS_TO_EXTRACT)

    if all_logs_data:
        print(f"Total de {len(all_logs_data)} logs de interação encontrados (todos).")

        # Exemplo: Imprimir os primeiros 2 logs de todos os logs
        for i, log_entry in enumerate(all_logs_data[:2]):
            print(f"\nLog (Geral) {i + 1}:")
            for key, value in log_entry.items():
                print(f"  {key}: {value}")

        # Salvar todos os logs em JSON
        save_to_json(all_logs_data, "interaction_logs_TODOS.json")

        # Salvar todos os logs em CSV
        save_to_csv(all_logs_data, "interaction_logs_TODOS.csv", columns=DEFAULT_COLUMNS_TO_EXTRACT)
    else:
        print("Nenhum log de interação encontrado (geral).")

    print("\n" + "=" * 50 + "\n")

    # --- Exemplo de busca com filtro de prazo ---
    print("--- Buscando logs de interação com filtro de prazo ---")
    start_date_filter = "2025-05-28 00:00:00"
    end_date_filter = "2025-05-30 12:00:00"

    filtered_logs_data = fetch_interaction_logs(DB_PATH,
                                                DEFAULT_COLUMNS_TO_EXTRACT,
                                                start_datetime=start_date_filter,
                                                end_datetime=end_date_filter)

    if filtered_logs_data:
        print(
            f"Total de {len(filtered_logs_data)} logs de interação encontrados para o período de {start_date_filter} a {end_date_filter}.")

        # Exemplo: Imprimir os primeiros 2 logs filtrados
        for i, log_entry in enumerate(filtered_logs_data[:2]):
            print(f"\nLog (Filtrado) {i + 1}:")
            for key, value in log_entry.items():
                print(f"  {key}: {value}")

        # Salvar logs filtrados em JSON
        save_to_json(filtered_logs_data,
                     f"interaction_logs_filtrado_{start_date_filter.split(' ')[0]}_a_{end_date_filter.split(' ')[0]}.json")

        # Salvar logs filtrados em CSV
        save_to_csv(filtered_logs_data,
                    f"interaction_logs_filtrado_{start_date_filter.split(' ')[0]}_a_{end_date_filter.split(' ')[0]}.csv",
                    columns=DEFAULT_COLUMNS_TO_EXTRACT)
    else:
        print(f"Nenhum log de interação encontrado para o período de {start_date_filter} a {end_date_filter}.")