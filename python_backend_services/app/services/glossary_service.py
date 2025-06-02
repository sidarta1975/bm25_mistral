# python_backend_services/app/services/glossary_service.py
import csv
import logging
import os
from typing import Dict, List, Optional, Any, Set
import re

try:
    from python_backend_services.app.core.config import settings
except ImportError:
    # Fallback para execução standalone ou testes unitários
    print("glossary_service.py: WARNING - Could not import 'settings' from project structure. Using fallback.")


    class MockSettingsGlossary:
        GLOSSARY_FILE_PATH = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', '..', 'shared_data', 'global_glossary.tsv')
        )
        LOG_LEVEL = "DEBUG"


    settings = MockSettingsGlossary()
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=settings.LOG_LEVEL.upper(),
                            format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')

logger = logging.getLogger(__name__)


class GlossaryService:
    def __init__(self, glossary_tsv_path: Optional[str] = None):
        self.glossary_tsv_path = glossary_tsv_path or settings.GLOSSARY_FILE_PATH [cite: 1]
        # Estrutura: { "termo_normalizado": {"termo_original": "Termo Original", "summary_tec": "...", "summary_public": "...", ...} }
        self.glossary_data: Dict[str, Dict[str, Any]] = {}
        self.normalized_terms_set: Set[str] = set()

        # ATUALIZADO: Campos esperados do global_glossary.tsv conforme a nova estrutura A-F
        # Coluna A: "Nº", Coluna B: "termo_juridico", Coluna C: "summary_tec",
        # Coluna D: "summary_public", Coluna E: "sub_areas_of_law", Coluna F: "legal_domain"
        self._expected_fields_header = [
            "Nº", "termo_juridico", "summary_tec", "summary_public",
            "sub_areas_of_law", "legal_domain"
        ]
        self._term_column_name = "termo_juridico"  # Coluna B, que contém o termo principal [cite: 1]

        self._load_glossary()

    def _normalize_term(self, term: str) -> str:
        """Normaliza um termo para busca (lowercase, etc.)."""
        if not term:
            return ""
        return term.strip().lower()

    def _load_glossary(self):
        """
        Carrega os termos do glossário do arquivo TSV.
        Armazena dados detalhados por termo e um conjunto de termos normalizados para busca.
        Utiliza os nomes de coluna definidos em self._expected_fields_header.
        """
        if not os.path.exists(self.glossary_tsv_path): # Verifica se o arquivo de glossário existe [cite: 1]
            logger.error(f"Arquivo de glossário NÃO ENCONTRADO em: {self.glossary_tsv_path}")
            return

        try:
            with open(self.glossary_tsv_path, 'r', encoding='utf-8') as f: # Abre o arquivo de glossário [cite: 1]
                # Usar DictReader garante que acessamos as colunas pelos nomes do cabeçalho do arquivo TSV.
                reader = csv.DictReader(f, delimiter='\t')

                # Validação opcional do cabeçalho (pode ser útil para garantir consistência)
                if reader.fieldnames:
                    missing_headers = [h for h in self._expected_fields_header if h not in reader.fieldnames]
                    if missing_headers:
                        logger.warning(f"Cabeçalho do glossário em '{self.glossary_tsv_path}' "
                                       f"não contém todas as colunas esperadas. Faltando: {missing_headers}. "
                                       f"Cabeçalho real: {reader.fieldnames}. Tentando prosseguir com as colunas disponíveis.")
                else:
                    logger.error(f"Não foi possível ler o cabeçalho do arquivo de glossário: {self.glossary_tsv_path}")
                    return

                processed_count = 0
                for row_num, row_dict in enumerate(reader, 1):
                    term_original = row_dict.get(self._term_column_name, "").strip() # Obtém o termo da coluna especificada [cite: 1]
                    if not term_original:
                        # logger.debug(f"Linha {row_num + 1} do glossário sem '{self._term_column_name}'. Pulando.")
                        continue

                    term_normalized = self._normalize_term(term_original) # Normaliza o termo [cite: 1]
                    if not term_normalized:
                        continue

                    # Armazena todos os campos do TSV, mantendo o termo original para exibição
                    # e o termo normalizado como chave para busca case-insensitive.
                    term_details = {"termo_original": term_original} # Adiciona o termo original aos detalhes [cite: 1]
                    # Carrega todos os campos presentes no TSV para o dicionário term_details
                    for field_from_file in reader.fieldnames: # Itera sobre as colunas reais do arquivo
                        clean_field_name = field_from_file.strip()
                        term_details[clean_field_name] = row_dict.get(field_from_file, "").strip()


                    if term_normalized in self.glossary_data: # Verifica termos duplicados [cite: 1]
                        logger.warning(f"Termo duplicado (normalizado) encontrado no glossário: '{term_normalized}'. "
                                       f"Sobrescrevendo com dados da linha {row_num + 1}.")

                    self.glossary_data[term_normalized] = term_details # Armazena os detalhes do termo [cite: 1]
                    self.normalized_terms_set.add(term_normalized) # Adiciona o termo normalizado ao conjunto [cite: 1]
                    processed_count += 1

            logger.info(f"Glossário carregado de '{self.glossary_tsv_path}'. "
                        f"{len(self.glossary_data)} termos únicos (normalizados) processados.") # Loga o número de termos carregados [cite: 1]
            if not self.glossary_data:
                logger.warning("Nenhum termo foi carregado do glossário. Verifique o arquivo e o formato.")

        except FileNotFoundError:
            logger.error(
                f"Erro crítico: Arquivo de glossário não encontrado em _load_glossary: {self.glossary_tsv_path}",
                exc_info=True)
        except Exception as e:
            logger.error(f"Erro ao carregar o glossário de '{self.glossary_tsv_path}': {e}", exc_info=True)

    def get_term_details(self, term: str) -> Optional[Dict[str, Any]]:
        """Retorna os detalhes de um termo normalizado, se existir."""
        normalized_term = self._normalize_term(term) # Normaliza o termo para busca [cite: 1]
        return self.glossary_data.get(normalized_term) # Retorna os detalhes do termo do dicionário glossary_data [cite: 1]

    def find_terms_in_text(self, text: str, include_details: bool = False) -> List[Dict[str, Any]] | List[str]:
        """
        Encontra termos do glossário presentes no texto.
        Usa regex para encontrar termos como palavras inteiras, case-insensitive.

        Args:
            text (str): O texto a ser analisado.
            include_details (bool): Se True, retorna uma lista de dicionários com detalhes dos termos
                                    (incluindo "Nº", "termo_juridico", "summary_tec", "summary_public",
                                    "sub_areas_of_law", "legal_domain" e quaisquer outras colunas do TSV).
                                    Se False, retorna uma lista de strings com os termos originais encontrados.

        Returns:
            List[Dict[str, Any]] | List[str]: Lista dos termos encontrados.
        """
        if not text or not self.normalized_terms_set: # Retorna lista vazia se o texto ou o conjunto de termos normalizados estiverem vazios [cite: 1]
            return []

        found_terms_output: List[Dict[str, Any]] | List[str] = []
        normalized_terms_found_in_this_text: Set[str] = set()

        # text_lower = text.lower() # Não é estritamente necessário aqui por causa do (?i) no regex

        for term_normalized in self.normalized_terms_set: # Itera sobre os termos normalizados do glossário [cite: 1]
            if term_normalized in normalized_terms_found_in_this_text:
                continue

            term_escaped_for_regex = re.escape(term_normalized) # Escapa caracteres especiais para o regex [cite: 1]
            pattern = r"(?i)\b" + term_escaped_for_regex + r"\b" # Cria o padrão regex para busca case-insensitive de palavras inteiras [cite: 1]

            match = re.search(pattern, text) # Busca o padrão no texto [cite: 1]
            if match:
                term_details = self.glossary_data.get(term_normalized) # Obtém os detalhes do termo se encontrado [cite: 1]
                if term_details:
                    if include_details: # Se include_details for True, adiciona o dicionário completo de detalhes [cite: 1]
                        found_terms_output.append(term_details)
                    else: # Caso contrário, adiciona apenas o termo original [cite: 1]
                        found_terms_output.append(term_details.get("termo_juridico", term_normalized)) # Prioriza "termo_juridico" que é o nome da coluna B
                    normalized_terms_found_in_this_text.add(term_normalized)

        return found_terms_output

    def get_all_terms_normalized(self) -> Set[str]:
        """Retorna um conjunto de todos os termos normalizados do glossário."""
        return self.normalized_terms_set

    def get_all_terms_with_details(self) -> Dict[str, Dict[str, Any]]:
        """Retorna todos os dados do glossário."""
        return self.glossary_data


if __name__ == '__main__':
    logger.info("Testando GlossaryService standalone...")

    actual_glossary_path = settings.GLOSSARY_FILE_PATH # Usa o caminho do arquivo de glossário das configurações [cite: 1]
    logger.info(f"Caminho do arquivo de glossário para o teste: {actual_glossary_path}")

    # Cria um arquivo de glossário de exemplo se não existir para fins de teste
    if not os.path.exists(actual_glossary_path):
        logger.error(f"ARQUIVO DE TESTE NÃO ENCONTRADO: {actual_glossary_path}. Crie um arquivo 'global_glossary.tsv' "
                     f"em shared_data com o cabeçalho e alguns dados para testar.")
        logger.info("Criando um arquivo de glossário de exemplo para teste...")
        os.makedirs(os.path.dirname(actual_glossary_path), exist_ok=True)
        with open(actual_glossary_path, 'w', encoding='utf-8') as f_example:
            # Cabeçalho conforme a nova estrutura A-F
            f_example.write(
                "Nº\ttermo_juridico\tsummary_tec\tsummary_public\tsub_areas_of_law\tlegal_domain\n")
            f_example.write(
                "1\tDano Moral\tLesão a bem jurídico extrapatrimonial.\tQuando alguém se sente ofendido moralmente.\tObrigações\tDireito Civil\n")
            f_example.write(
                "2\tUsucapião\tModo de aquisição de propriedade pela posse prolongada.\tTornar-se dono de algo pelo tempo de uso.\tPropriedade\tDireito Civil\n")
            f_example.write(
                "3\tPensão Alimentícia\tValor pago para suprir necessidades básicas.\tDinheiro para sustento, geralmente para filhos.\tAlimentos\tDireito de Família\n")

    try:
        gs = GlossaryService(glossary_tsv_path=actual_glossary_path) # Instancia o GlossaryService [cite: 1]

        if gs.glossary_data: # Verifica se o glossário foi carregado [cite: 1]
            logger.info(f"Glossário carregado com {len(gs.glossary_data)} termos.")

            sample_text_1 = "O caso envolve dano moral e também questões de usucapião."
            logger.info(f"Procurando termos no texto de exemplo: '{sample_text_1}'")

            found1_details = gs.find_terms_in_text(sample_text_1, include_details=True) # Busca termos com detalhes [cite: 1]
            print(f"Termos encontrados em '{sample_text_1}' (com detalhes):")
            if found1_details:
                for term_info in found1_details:
                    # Exibe os campos relevantes do glossário
                    print(f"- {term_info.get('termo_juridico')}: Def. Téc.: '{term_info.get('summary_tec')}', Domínio: '{term_info.get('legal_domain')}'")
            else:
                print("Nenhum termo.")

            found1_terms_only = gs.find_terms_in_text(sample_text_1, include_details=False) # Busca apenas os nomes dos termos [cite: 1]
            print(f"\nTermos encontrados em '{sample_text_1}' (apenas termos):")
            if found1_terms_only:
                for term_val in found1_terms_only:
                    print(f"- {term_val}")
            else:
                print("Nenhum termo.")
        else:
            logger.error("Glossário NÃO foi carregado ou está vazio. Verifique o caminho e o conteúdo do arquivo.")

    except Exception as e_test:
        logger.error(f"ERRO INESPERADO durante o teste do GlossaryService: {e_test}", exc_info=True)