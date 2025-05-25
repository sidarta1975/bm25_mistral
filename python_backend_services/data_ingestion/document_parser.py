# python_backend_services/data_ingestion/document_parser.py
import csv
import os
import logging
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)


class DocumentParser:
    def __init__(self, metadata_tsv_path: str, source_docs_base_path: str):
        """
        Initializes the DocumentParser.

        Args:
            metadata_tsv_path (str): Path to the TSV file containing document metadata.
            source_docs_base_path (str): Base directory path where .txt source documents are located.
        """
        if not os.path.exists(metadata_tsv_path):
            logger.error(f"Metadata TSV file not found at: {metadata_tsv_path}")
            raise FileNotFoundError(f"Metadata TSV file not found at: {metadata_tsv_path}")
        if not os.path.isdir(source_docs_base_path):
            logger.error(f"Source documents base path is not a valid directory: {source_docs_base_path}")
            raise NotADirectoryError(f"Source documents base path is not a valid directory: {source_docs_base_path}")

        self.metadata_tsv_path = metadata_tsv_path
        self.source_docs_base_path = source_docs_base_path
        # Define os campos esperados no TSV, correspondendo à sua especificação
        self.expected_fields = [
            "document_id", "file_name", "content_path", "document_title", "summary",
            "first_lines", "document_category", "document_type", "legal_action",
            "legal_domain", "sub_areas_of_law", "jurisprudence_court", "version"
        ]

    def parse_documents(self) -> List[Dict[str, Any]]:
        """
        Parses all documents listed in the metadata TSV file.
        Reads their content from .txt files and combines with metadata.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                                 represents a document with its metadata and content.
        """
        documents = []
        logger.info(f"Starting document parsing from TSV: {self.metadata_tsv_path}")

        try:
            with open(self.metadata_tsv_path, mode='r', encoding='utf-8') as tsvfile:
                reader = csv.DictReader(tsvfile, delimiter='\t')

                # Validar cabeçalho do TSV
                if not all(field in reader.fieldnames for field in self.expected_fields):
                    missing_fields = [field for field in self.expected_fields if field not in reader.fieldnames]
                    logger.error(
                        f"TSV file is missing expected columns: {missing_fields}. Found columns: {reader.fieldnames}")
                    # Você pode optar por lançar um erro aqui se preferir
                    # raise ValueError(f"TSV file is missing expected columns: {missing_fields}")
                    # Por enquanto, vamos logar o erro e continuar tentando processar com os campos disponíveis.

                for row_num, row in enumerate(reader, 1):
                    doc_data = {}
                    for field in self.expected_fields:
                        doc_data[field] = row.get(field, None)  # Pega o valor ou None se o campo não existir na linha

                    file_name = doc_data.get("file_name")
                    content_path_relative = doc_data.get("content_path")  # Pode ser None ou vazio

                    if not file_name:
                        logger.warning(f"Skipping row {row_num} due to missing 'file_name' in TSV.")
                        continue

                    # Construir o caminho do conteúdo
                    # Prioriza content_path se fornecido, senão usa file_name diretamente na base
                    actual_content_path = ""
                    if content_path_relative:
                        # Se content_path for absoluto, use-o. Senão, junte com a base.
                        if os.path.isabs(content_path_relative):
                            actual_content_path = content_path_relative
                        else:
                            actual_content_path = os.path.join(self.source_docs_base_path, content_path_relative)
                    else:  # Se content_path não for fornecido, usa file_name
                        actual_content_path = os.path.join(self.source_docs_base_path, file_name)

                    doc_data["content_path_resolved"] = actual_content_path  # Armazena o caminho resolvido

                    try:
                        with open(actual_content_path, 'r', encoding='utf-8') as f_content:
                            doc_data["content"] = f_content.read()
                    except FileNotFoundError:
                        logger.warning(
                            f"Content file not found for '{file_name}' at path '{actual_content_path}'. Skipping document content for this entry.")
                        doc_data["content"] = None  # Ou pode optar por pular o documento inteiro
                    except Exception as e:
                        logger.error(f"Error reading content file '{actual_content_path}' for '{file_name}': {e}")
                        doc_data["content"] = None

                    # Garante que o 'id' para o Elasticsearch seja o 'document_id' do TSV
                    # Se 'document_id' não estiver no TSV ou for vazio, podemos usar file_name como fallback,
                    # mas o ideal é que document_id seja único e presente.
                    es_doc_id = doc_data.get("document_id")
                    if not es_doc_id:
                        logger.warning(
                            f"Row {row_num} ('{file_name}') has no 'document_id'. Using 'file_name' as fallback ID for Elasticsearch.")
                        es_doc_id = file_name  # Fallback, mas pode não ser ideal se file_name não for único.

                    doc_data["id"] = es_doc_id  # Campo 'id' que o ElasticsearchService.bulk_index_documents espera

                    documents.append(doc_data)
                    if row_num % 100 == 0:
                        logger.info(f"Processed {row_num} rows from TSV...")

        except FileNotFoundError:
            logger.error(f"Could not open metadata TSV file: {self.metadata_tsv_path}")
            # Re-raise para que o chamador saiba que a operação falhou criticamente
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while parsing TSV or reading documents: {e}")
            # Também pode ser útil re-raise aqui dependendo da estratégia de tratamento de erro
            # raise

        logger.info(f"Finished parsing. Total documents processed: {len(documents)}")
        return documents


if __name__ == '__main__':
    # Exemplo de como usar o DocumentParser
    # Crie um diretório 'test_docs' e um 'test_metadata.tsv' para testar

    # Setup básico de logging para o teste
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Supondo que shared_data e source_documents estão no mesmo nível que data_ingestion
    # e todos dentro de python_backend_services
    project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))

    # Caminhos para teste (ajuste conforme sua estrutura se este script for movido)
    # Idealmente, estes viriam do config.py no run_ingestion.py
    test_tsv_path = os.path.join(project_root, "shared_data", "relatorio_classificacao_teste.tsv")
    # O caminho base para os documentos .txt
    # Este é o caminho que você especificou: /home/sidarta/PycharmProjects/bm25_mistral/python_backend_services/source_documents/petitions
    test_docs_base = "/home/sidarta/PycharmProjects/bm25_mistral/python_backend_services/source_documents/petitions"
    # Para teste local, você pode criar um subdiretório e um TSV de teste:
    # test_docs_base = os.path.join(project_root, "source_documents_test_parser")

    # Certifique-se de que o diretório base dos documentos de teste exista
    if not os.path.isdir(test_docs_base):
        logger.error(f"Diretório de documentos de teste não encontrado: {test_docs_base}. Crie-o para testar.")
        # os.makedirs(test_docs_base, exist_ok=True) # Descomente para criar se não existir

    # Criar um TSV de exemplo para teste se não existir
    if not os.path.exists(test_tsv_path):
        logger.info(f"Criando TSV de teste em: {test_tsv_path}")
        os.makedirs(os.path.dirname(test_tsv_path), exist_ok=True)
        with open(test_tsv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            # Cabeçalho conforme self.expected_fields
            writer.writerow([
                "document_id", "file_name", "content_path", "document_title", "summary",
                "first_lines", "document_category", "document_type", "legal_action",
                "legal_domain", "sub_areas_of_law", "jurisprudence_court", "version"
            ])
            # Adicione algumas linhas de exemplo. Garanta que os file_name existam em test_docs_base
            # Exemplo: se você tem /home/sidarta/.../petitions/pet001.txt
            # writer.writerow([
            #     "pet001_id", "pet001.txt", "", "Título da Petição 001", "Resumo da pet001.",
            #     "Primeiras linhas da pet001...", "Petição", "Petição Inicial", "Ação de Teste",
            #     "Direito Civil Teste", "[\"Subarea1\", \"Subarea2\"]", "TJTESTE", "1.0"
            # ])
            # writer.writerow([
            #     "doc002_id", "doc002.txt", "subfolder/doc002.txt", "Contrato de Exemplo", "Resumo do contrato.",
            #     "Este é um contrato...", "Contrato", "Contrato de Locação", "",
            #     "Direito Imobiliário", "[]", "", "alpha"
            # ])
        # Lembre-se de criar os arquivos .txt correspondentes em test_docs_base
        # Ex: test_docs_base/pet001.txt com algum conteúdo.
        # Ex: test_docs_base/subfolder/doc002.txt (crie 'subfolder' também)
        logger.info("Crie arquivos .txt correspondentes aos file_name no TSV de teste para um teste completo.")

    if os.path.exists(test_tsv_path) and os.path.isdir(test_docs_base):
        try:
            parser = DocumentParser(metadata_tsv_path=test_tsv_path, source_docs_base_path=test_docs_base)
            parsed_docs = parser.parse_documents()
            if parsed_docs:
                logger.info(f"\nExemplo do primeiro documento parseado ({len(parsed_docs)} total):")
                for key, value in parsed_docs[0].items():
                    if key == "content" and value is not None:
                        logger.info(f"  {key}: {value[:100]}...")  # Mostra apenas os primeiros 100 chars do conteúdo
                    else:
                        logger.info(f"  {key}: {value}")
            else:
                logger.info("Nenhum documento foi parseado. Verifique o TSV e os caminhos dos arquivos.")
        except FileNotFoundError as e:
            logger.error(f"Erro no teste do DocumentParser (Arquivo não encontrado): {e}")
        except NotADirectoryError as e:
            logger.error(f"Erro no teste do DocumentParser (Diretório base inválido): {e}")
        except Exception as e:
            logger.error(f"Erro inesperado no teste do DocumentParser: {e}", exc_info=True)
    else:
        logger.warning(
            f"TSV de teste ({test_tsv_path}) ou diretório de documentos de teste ({test_docs_base}) não encontrado. Pulando exemplo de uso.")