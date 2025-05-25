# python_backend_services/data_ingestion/document_parser.py
import csv
import os
import logging
from typing import List, Dict, Any

# Usar __name__ é uma boa prática para o logger do módulo
logger = logging.getLogger(__name__)


class DocumentParser:
    def __init__(self, metadata_tsv_path: str, source_docs_base_path: str):
        if not os.path.exists(metadata_tsv_path):
            msg = f"Arquivo TSV de metadados NÃO ENCONTRADO em: {metadata_tsv_path}"
            logger.error(msg)
            raise FileNotFoundError(msg)  # Interrompe se o TSV não existe
        if not os.path.isdir(source_docs_base_path):
            msg = f"Diretório base dos documentos fonte NÃO É VÁLIDO: {source_docs_base_path}"
            logger.error(msg)
            raise NotADirectoryError(msg)  # Interrompe se o diretório base não existe

        self.metadata_tsv_path = metadata_tsv_path
        self.source_docs_base_path = source_docs_base_path
        # Campos que esperamos encontrar no cabeçalho do arquivo TSV
        self.expected_fields = [
            "document_id", "file_name", "content_path", "document_title", "summary",
            "first_lines", "document_category", "document_type", "legal_action",
            "legal_domain", "sub_areas_of_law", "jurisprudence_court", "version"
        ]
        logger.info(
            f"DocumentParser inicializado. TSV: '{self.metadata_tsv_path}', Base Docs: '{self.source_docs_base_path}'")

    def parse_documents(self) -> List[Dict[str, Any]]:
        documents = []
        logger.info(f"Iniciando parsing de documentos do TSV: {self.metadata_tsv_path}")

        try:
            with open(self.metadata_tsv_path, mode='r', encoding='utf-8') as tsvfile:
                reader = csv.DictReader(tsvfile, delimiter='\t')

                header = reader.fieldnames
                if not header:  # Verifica se o TSV tem cabeçalho
                    logger.error(f"Arquivo TSV '{self.metadata_tsv_path}' está vazio ou não possui cabeçalho.")
                    return []

                    # Valida o cabeçalho do TSV, mas continua se alguns campos estiverem faltando
                missing_fields_in_header = [field for field in self.expected_fields if field not in header]
                if missing_fields_in_header:
                    logger.warning(
                        f"AVISO: As seguintes colunas esperadas não foram encontradas no cabeçalho do TSV: {missing_fields_in_header}. Campos encontrados: {header}. O parser tentará prosseguir com os campos disponíveis para cada linha.")

                for row_num, row_dict_from_tsv in enumerate(reader, 1):
                    # Cria um dicionário para os dados do documento, preenchendo com None se o campo não existir na linha
                    doc_data = {field_name: row_dict_from_tsv.get(field_name) for field_name in self.expected_fields}

                    file_name = doc_data.get("file_name")

                    # Pula a linha e loga um aviso se 'file_name' não estiver presente ou for uma string vazia
                    if not file_name or not file_name.strip():
                        # Log com mais detalhes da linha problemática
                        problematic_id = doc_data.get("document_id", "ID não informado")
                        logger.warning(
                            f"Pulando linha {row_num} do TSV por 'file_name' ausente ou vazio. document_id (se houver): '{problematic_id}'. Dados da linha: { {k: v for k, v in row_dict_from_tsv.items() if v is not None} }")
                        continue  # Pula para a próxima linha do TSV

                    content_path_from_tsv = doc_data.get("content_path")  # Pode ser None ou uma string vazia
                    actual_content_path = ""

                    # Constrói o caminho absoluto para o arquivo .txt
                    if content_path_from_tsv and content_path_from_tsv.strip():  # Se content_path foi fornecido e não é só espaço
                        if os.path.isabs(content_path_from_tsv):
                            actual_content_path = content_path_from_tsv
                        else:  # É um caminho relativo
                            actual_content_path = os.path.join(self.source_docs_base_path, content_path_from_tsv)
                    else:  # Se content_path não for fornecido ou for vazio, usa file_name diretamente na base
                        actual_content_path = os.path.join(self.source_docs_base_path, file_name)

                    doc_data["content_path_resolved"] = actual_content_path  # Guarda o caminho resolvido

                    # Tenta ler o conteúdo do arquivo .txt
                    doc_content = None  # Inicializa como None para garantir que o campo exista
                    try:
                        with open(actual_content_path, 'r', encoding='utf-8') as f_content:
                            doc_content = f_content.read()
                    except FileNotFoundError:
                        # Loga um aviso mas NÃO PARA o processo, apenas o 'content' será None
                        logger.warning(
                            f"Arquivo .txt NÃO ENCONTRADO para '{file_name}' no caminho '{actual_content_path}' (referente à linha {row_num} do TSV). O campo 'content' será definido como None para este documento.")
                    except Exception as e:
                        # Loga outros erros de leitura mas também NÃO PARA
                        logger.error(
                            f"Erro ao ler o arquivo .txt '{actual_content_path}' para o documento '{file_name}' (linha {row_num} do TSV): {e}",
                            exc_info=False)  # exc_info=False para não poluir muito com tracebacks
                    doc_data["content"] = doc_content

                    # Define o 'id' para o Elasticsearch, priorizando 'document_id' do TSV
                    # Este 'id' será usado como _id no Elasticsearch
                    es_doc_id = doc_data.get("document_id")
                    if not es_doc_id or not es_doc_id.strip():  # Se 'document_id' estiver vazio ou for None
                        logger.warning(
                            f"Linha {row_num} do TSV (arquivo: '{file_name}') não possui 'document_id' ou está vazio. Usando 'file_name' ('{file_name}') como fallback para o _id no Elasticsearch. ATENÇÃO: 'file_name' deve ser único se isso ocorrer com frequência.")
                        es_doc_id = file_name  # Usa file_name como fallback

                    doc_data[
                        "id"] = es_doc_id  # Adiciona o campo 'id' que o ElasticsearchService.bulk_index_documents espera
                    documents.append(doc_data)

                    if row_num % 20 == 0:  # Log de progresso a cada 20 documentos
                        logger.info(f"Processadas {row_num} linhas do TSV...")

        except FileNotFoundError:  # Erro ao abrir o próprio arquivo TSV (crítico)
            logger.error(
                f"FALHA CRÍTICA: Não foi possível abrir o arquivo de metadados TSV durante o parsing: {self.metadata_tsv_path}",
                exc_info=True)
            raise  # Re-lança a exceção, pois sem o TSV não há como prosseguir
        except Exception as e:  # Outros erros inesperados durante o parsing do TSV (crítico)
            logger.error(f"FALHA CRÍTICA: Erro inesperado durante o parsing do TSV ou leitura dos documentos: {e}",
                         exc_info=True)
            raise  # Re-lança

        logger.info(f"Parsing concluído. Total de documentos preparados (com ou sem conteúdo): {len(documents)}")
        return documents


# Bloco if __name__ == '__main__': para teste (mantido da versão anterior, com logging mais verboso)
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,  # DEBUG para ver todos os logs do parser durante o teste
                        format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')

    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_services_root = os.path.dirname(current_script_dir)  # ..
    project_root = os.path.dirname(project_services_root)  # ../.. (bm25_mistral)

    # Constrói caminhos a partir da raiz do projeto inferida
    # Garante que funcione mesmo se a estrutura de pastas mudar um pouco
    test_tsv_path = os.path.join(project_root, "python_backend_services", "shared_data", "relatorio_classificacao.tsv")
    test_docs_base = os.path.join(project_root, "python_backend_services", "source_documents", "petitions")

    print(f"--- Teste Standalone DocumentParser ---")
    print(f"Tentando usar TSV de: {test_tsv_path}")
    print(f"Tentando usar Base de Documentos de: {test_docs_base}")

    # Cria um TSV de teste e alguns arquivos .txt se não existirem para facilitar o teste standalone
    os.makedirs(os.path.dirname(test_tsv_path), exist_ok=True)
    os.makedirs(test_docs_base, exist_ok=True)

    if not os.path.exists(test_tsv_path):
        print(f"Criando TSV de teste em: {test_tsv_path}")
        with open(test_tsv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow([
                "document_id", "file_name", "content_path", "document_title", "summary",
                "first_lines", "document_category", "document_type", "legal_action",
                "legal_domain", "sub_areas_of_law", "jurisprudence_court", "version"
            ])
            writer.writerow([  # Linha válida
                "pet001_id_test", "pet001_test.txt", "", "Título Teste 1", "Resumo 1",
                "Linhas 1...", "Petição", "Inicial", "Ação X", "Civil", "Contratos", "", "v1"
            ])
            writer.writerow([  # Linha com file_name faltando (será pulada)
                "pet002_id_test", "", "", "Título Teste 2", "Resumo 2",
                "Linhas 2...", "Petição", "Contestação", "Defesa Y", "Trabalho", "", "", "v2"
            ])
            writer.writerow([  # Linha válida, mas arquivo .txt não existirá (content será None)
                "pet003_id_test", "pet003_nao_existe.txt", "", "Título Teste 3", "Resumo 3",
                "Linhas 3...", "Contrato", "Locação", None, "Imobiliário", "Aluguel", "", "v3"
            ])
        # Cria o arquivo .txt para a primeira linha de teste
        with open(os.path.join(test_docs_base, "pet001_test.txt"), 'w', encoding='utf-8') as f_txt:
            f_txt.write("Este é o conteúdo do arquivo pet001_test.txt.")
        print("TSV de teste e arquivo .txt de exemplo criados.")

    if not os.path.exists(test_tsv_path):
        print(f"AVISO: Arquivo TSV de teste NÃO ENCONTRADO em {test_tsv_path} mesmo após tentativa de criação.")
    elif not os.path.isdir(test_docs_base):
        print(
            f"AVISO: Diretório base de documentos de teste NÃO ENCONTRADO em {test_docs_base} mesmo após tentativa de criação.")
    else:
        try:
            parser = DocumentParser(metadata_tsv_path=test_tsv_path, source_docs_base_path=test_docs_base)
            parsed_docs = parser.parse_documents()
            if parsed_docs:
                print(f"\nSucesso! {len(parsed_docs)} documento(s) foram preparados para indexação.")
                print("Exemplo dos primeiros documentos (se houver):")
                for i, doc_example in enumerate(parsed_docs[:2]):  # Mostra até 2 exemplos
                    print(f"\n--- Documento Exemplo {i + 1} ---")
                    for key, value in doc_example.items():
                        display_value = str(value)[:70] + '...' if isinstance(value, str) and len(value) > 70 else value
                        print(f"  {key}: {display_value}")
            else:
                print("Nenhum documento foi parseado com sucesso. Verifique os logs acima e o arquivo TSV.")
        except Exception as e:
            print(f"Erro no teste do DocumentParser: {e}", exc_info=True)