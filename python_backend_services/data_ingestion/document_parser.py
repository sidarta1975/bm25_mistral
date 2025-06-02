# python_backend_services/data_ingestion/document_parser.py
import csv
import os
import logging
from typing import List, Dict, Any, Optional

try:
    from python_backend_services.app.core.config import settings
except ImportError:
    settings = None
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level="INFO", format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
    print("document_parser.py: WARNING - Could not import 'settings' from project structure.")

if not logging.getLogger().hasHandlers():
    log_level_to_use = "INFO"
    if settings and hasattr(settings, 'LOG_LEVEL'):
        log_level_to_use = settings.LOG_LEVEL.upper()

    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    module_logger_for_setup = logging.getLogger(__name__)
    for handler in module_logger_for_setup.handlers[:]:
        module_logger_for_setup.removeHandler(handler)

    logging.basicConfig(level=log_level_to_use,
                        format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
                        force=True)

logger = logging.getLogger(__name__)


class DocumentParser:
    def __init__(self, metadata_tsv_path: str, source_docs_base_path: str):
        if not os.path.isfile(metadata_tsv_path):
            msg = f"Arquivo TSV de metadados NÃO ENCONTRADO ou não é um arquivo: {metadata_tsv_path}"
            logger.error(msg)
            raise FileNotFoundError(msg)
        if not os.path.isdir(source_docs_base_path):
            msg = f"Diretório base dos documentos fonte NÃO É VÁLIDO ou não existe: {source_docs_base_path}"
            logger.error(msg)
            raise NotADirectoryError(msg)

        self.metadata_tsv_path = metadata_tsv_path
        self.source_docs_base_path = source_docs_base_path

        self.expected_fields_from_tsv = [
            "document_id", "file_name", "name_text", "legal_domain",
            "sub_areas_of_law", "summary", "primeiras_20_linhas"
        ]
        self.optional_intermediate_keys = [
            "content_path", "document_title", "first_lines", "document_category",
            "document_type", "legal_action", "jurisprudence_court", "version"
        ]

        logger.info(
            f"DocumentParser inicializado. TSV: '{self.metadata_tsv_path}', Base Docs: '{self.source_docs_base_path}'"
        )
        logger.info(f"Campos esperados do cabeçalho do TSV: {self.expected_fields_from_tsv}")

    def _read_file_content(self, file_path: str) -> Optional[str]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            logger.warning(f"Arquivo de conteúdo não encontrado: {file_path}")
            return None
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo de conteúdo {file_path}: {e}", exc_info=True)
            return None

    def parse_documents(self) -> List[Dict[str, Any]]:
        documents: List[Dict[str, Any]] = []
        processed_lines = 0
        skipped_lines_no_filename = 0
        content_files_not_found = 0
        header_validated = False
        actual_header_from_file: List[str] = []

        try:
            with open(self.metadata_tsv_path, 'r', encoding='utf-8', newline='') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')

                try:
                    actual_header_from_file = [h.strip() for h in next(reader)]
                    processed_lines += 1
                    missing_expected_cols = [
                        expected_col for expected_col in self.expected_fields_from_tsv
                        if expected_col not in actual_header_from_file
                    ]
                    if missing_expected_cols:
                        logger.error(
                            f"Cabeçalho do TSV '{self.metadata_tsv_path}' não contém todas as colunas esperadas.")
                        logger.error(f"Colunas esperadas (devem estar no TSV): {self.expected_fields_from_tsv}")
                        logger.error(f"Colunas faltando no arquivo TSV: {missing_expected_cols}")
                        logger.error(f"Cabeçalho obtido do arquivo: {actual_header_from_file}")
                    else:
                        header_validated = True
                        logger.info(
                            f"Cabeçalho do TSV validado. Usando o cabeçalho do arquivo: {actual_header_from_file}")
                except StopIteration:
                    logger.warning(f"Arquivo TSV '{self.metadata_tsv_path}' está vazio.")
                    return documents

                if not header_validated:
                    logger.error(
                        "Não foi possível validar o cabeçalho do TSV. Encerrando o parsing.")
                    return documents

                for row_num, row_values in enumerate(reader, start=2):
                    processed_lines += 1
                    if len(row_values) != len(actual_header_from_file):
                        logger.warning(
                            f"Linha {row_num} do TSV tem {len(row_values)} colunas, esperado {len(actual_header_from_file)}. Pulando: {row_values}")
                        continue

                    row_data_dict = dict(zip(actual_header_from_file, row_values))
                    doc_data: Dict[str, Any] = {}

                    for field in actual_header_from_file:
                        doc_data[field] = row_data_dict.get(field, "").strip()

                    # Mapeamento e tratamento específico
                    doc_data["document_title"] = doc_data.get("name_text", "")
                    doc_data["first_lines"] = doc_data.get("primeiras_20_linhas", "")

                    # Tratar sub_areas_of_law para ser uma lista
                    sub_areas_str = doc_data.get("sub_areas_of_law", "")
                    if sub_areas_str:
                        # Assume que múltiplos valores são separados por ";"
                        doc_data["sub_areas_of_law"] = [area.strip() for area in sub_areas_str.split(';') if
                                                        area.strip()]
                    else:
                        doc_data["sub_areas_of_law"] = []

                    file_name = doc_data.get("file_name")
                    if not file_name:
                        logger.warning(f"Linha {row_num} do TSV não possui 'file_name'. Pulando.")
                        skipped_lines_no_filename += 1
                        continue

                    relative_content_path = doc_data.get("content_path")
                    if not relative_content_path or not str(relative_content_path).strip():
                        relative_content_path = file_name
                    doc_data["content_path"] = relative_content_path

                    full_content_file_path = os.path.join(self.source_docs_base_path, str(relative_content_path))
                    doc_data["full_text_content"] = self._read_file_content(full_content_file_path)

                    if doc_data["full_text_content"] is None:
                        content_files_not_found += 1
                        logger.warning(
                            f"Conteúdo não lido para document_id '{doc_data.get('document_id')}', file_name '{file_name}'")

                    doc_id_val = doc_data.get("document_id")
                    if not doc_id_val or not doc_id_val.strip():
                        logger.debug(
                            f"Linha {row_num} (file: '{file_name}') não tem 'document_id'. Usando 'file_name'.")
                        doc_data["document_id"] = file_name

                    for key in self.optional_intermediate_keys:
                        if key not in doc_data:
                            doc_data[key] = None

                    documents.append(doc_data)

                    if processed_lines % 100 == 0:
                        logger.info(f"Processadas {processed_lines} linhas do TSV...")

        except FileNotFoundError:
            logger.critical(f"FALHA CRÍTICA: TSV não encontrado: {self.metadata_tsv_path}", exc_info=True)
            raise
        except Exception as e:
            logger.critical(f"FALHA CRÍTICA: Erro no parsing do TSV: {e}", exc_info=True)
            raise

        logger.info(
            f"Parsing do TSV concluído. Linhas lidas: {processed_lines}. Documentos preparados: {len(documents)}."
        )
        if skipped_lines_no_filename > 0:
            logger.warning(f"{skipped_lines_no_filename} linhas puladas por 'file_name' ausente.")
        if content_files_not_found > 0:
            logger.warning(
                f"{content_files_not_found} arquivos .txt não encontrados (full_text_content será None).")

        return documents