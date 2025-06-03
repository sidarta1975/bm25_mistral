# python_backend_services/app/services/glossary_service.py
import logging
import gspread  # Certifique-se de que gspread está instalado no seu ambiente

# Não precisa importar 'settings' aqui diretamente se os parâmetros vêm do construtor

logger = logging.getLogger(__name__)


class GlossaryService:
    def __init__(self, service_account_path: str, gsheet_id: str, gsheet_tab_name: str):
        """
        Inicializa o GlossaryService para carregar dados de uma Planilha Google.

        Args:
            service_account_path (str): Caminho para o arquivo JSON da chave da conta de serviço GCP.
            gsheet_id (str): O ID da Planilha Google Sheets que contém o glossário.
            gsheet_tab_name (str): O nome da aba específica na planilha que contém os dados do glossário.
        """
        self.service_account_path = service_account_path
        self.gsheet_id = gsheet_id
        self.gsheet_tab_name = gsheet_tab_name
        self.glossary_data = self._load_glossary_data_from_gsheet()  # Carrega os dados na inicialização

        if self.glossary_data:
            logger.info(
                f"GlossaryService inicializado com sucesso. {len(self.glossary_data)} termos carregados do GSheet ID: {self.gsheet_id}, Aba: {self.gsheet_tab_name}"
            )
        else:
            logger.warning(
                f"GlossaryService inicializado, mas nenhum termo foi carregado do GSheet ID: {self.gsheet_id}, Aba: {self.gsheet_tab_name}. Verifique a planilha, permissões ou logs de erro."
            )

    def _load_glossary_data_from_gsheet(self) -> dict:
        """
        Carrega os dados do glossário da Planilha Google Sheets especificada.
        Assume que a primeira linha da aba é o cabeçalho.
        """
        logger.info(f"Tentando carregar dados do glossário do GSheet ID: {self.gsheet_id}, Aba: {self.gsheet_tab_name}")
        glossary_data_map = {}
        try:
            gc = gspread.service_account(filename=self.service_account_path)
            spreadsheet = gc.open_by_key(self.gsheet_id)
            worksheet = spreadsheet.worksheet(self.gsheet_tab_name)

            # get_all_records() converte as linhas em uma lista de dicionários,
            # usando a primeira linha como chaves.
            glossary_entries = worksheet.get_all_records()
            logger.info(f"Lidos {len(glossary_entries)} registros da planilha de glossário.")

            for entry_dict in glossary_entries:
                # Adapte os nomes das chaves ('termo_juridico', 'summary_tec', etc.)
                # para corresponder EXATAMENTE aos nomes das colunas na sua planilha de glossário.
                term = entry_dict.get('termo_juridico', '').strip().lower()
                if term:
                    glossary_data_map[term] = {
                        "summary_tec": entry_dict.get('summary_tec', ''),
                        "summary_public": entry_dict.get('summary_public', ''),
                        "sub_areas_of_law": entry_dict.get('sub_areas_of_law', ''),
                        "legal_domain": entry_dict.get('legal_domain', '')
                        # Adicione quaisquer outros campos que seu glossário utilize
                    }

            if not glossary_entries and worksheet.row_count > 0:  # Planilha tem linhas mas get_all_records() retornou vazio
                logger.warning(
                    f"Nenhum registro retornado por get_all_records() para o glossário. A primeira linha é um cabeçalho válido e há dados abaixo dela?")


        except FileNotFoundError:
            logger.error(
                f"ERRO CRÍTICO ao carregar glossário: Arquivo de credenciais '{self.service_account_path}' não encontrado.",
                exc_info=True)
            # Você pode querer levantar uma exceção aqui para impedir a inicialização da app se o glossário for essencial.
            # raise
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(
                f"ERRO CRÍTICO ao carregar glossário: Planilha com ID '{self.gsheet_id}' não encontrada. Verifique o ID e as permissões de compartilhamento com a conta de serviço.",
                exc_info=True)
            # raise
        except gspread.exceptions.WorksheetNotFound:
            logger.error(
                f"ERRO CRÍTICO ao carregar glossário: Aba com nome '{self.gsheet_tab_name}' não encontrada na planilha ID '{self.gsheet_id}'.",
                exc_info=True)
            # raise
        except Exception as e:
            logger.error(f"Erro inesperado ao carregar o glossário do Google Sheets: {e}", exc_info=True)
            # raise

        return glossary_data_map

    def find_terms_in_text(self, text: str, include_details: bool = False) -> list:
        """
        Encontra termos do glossário no texto fornecido.
        (Esta é uma implementação de exemplo, adapte à sua lógica original se diferente)
        """
        if not self.glossary_data:
            logger.warning("Tentativa de buscar termos, mas o glossário está vazio ou não foi carregado.")
            return []

        found_terms = []
        text_lower = text.lower()
        for term, details in self.glossary_data.items():
            if term in text_lower:  # Busca simples por substring
                if include_details:
                    # Cria uma cópia dos detalhes e adiciona o termo original para evitar modificar o dict do glossário
                    term_info = details.copy()
                    term_info["termo_original"] = term
                    found_terms.append(term_info)
                else:
                    found_terms.append(term)
        return found_terms

    # Adicione aqui quaisquer outros métodos que sua GlossaryService original possuía.
    # Por exemplo, um método para buscar a definição de um termo específico, etc.
    def get_term_details(self, term: str) -> dict | None:
        return self.glossary_data.get(term.lower().strip())