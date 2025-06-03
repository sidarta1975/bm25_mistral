# Dockerfile
# Localização: /home/sidarta/PycharmProjects/bm25_mistral_pq/python_backend_services/Dockerfile

# 1. Imagem Base
# Use uma imagem Python oficial. Python 3.11-slim é uma boa escolha por ser leve.
FROM python:3.11-slim-bullseye

# 2. Definir Variáveis de Ambiente
# Evita que o Python gere arquivos .pyc e garante que a saída seja enviada diretamente para o terminal (útil para logs do Docker)
ENV PYTHONDONTWRITEBYTECODE="1"
ENV PYTHONUNBUFFERED="1"

# Configurações para o Flask. A aplicação Flask é o objeto 'application' no arquivo 'app/main.py'.
# Dentro do container, o WORKDIR será /app, e o código será copiado para lá.
ENV FLASK_APP="app.main:application"
# Permite que a aplicação seja acessada de fora do container
ENV FLASK_RUN_HOST="0.0.0.0"
# Porta padrão que a aplicação Flask usará dentro do container
ENV FLASK_RUN_PORT="5000"

# Você pode definir outras variáveis de ambiente aqui se necessário,
# por exemplo, para substituir valores do config.py ou para configurar
# a conexão com Elasticsearch e Ollama no ambiente Docker.
# Exemplo:
# ENV ELASTICSEARCH_HOST="http://elasticsearch_container_name:9200"
# ENV OLLAMA_API_URL="http://ollama_container_name:11434/api/generate"

# 3. Definir o Diretório de Trabalho
# Todos os comandos subsequentes serão executados a partir deste diretório dentro da imagem.
WORKDIR /app

# 4. Instalar Dependências do Sistema (se houver)
# Se suas bibliotecas Python tiverem dependências de pacotes do sistema operacional, instale-as aqui.
# Exemplo:
# RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# 5. Copiar o arquivo requirements.txt e Instalar Dependências Python
# Copia o requirements.txt para o diretório de trabalho /app
# Presume-se que requirements.txt está na raiz do contexto de build (python_backend_services/)
COPY requirements.txt .

# Instala as dependências Python listadas no requirements.txt
# --no-cache-dir reduz o tamanho da imagem
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copiar o Código da Aplicação
# Copia todos os arquivos e diretórios do contexto de build (python_backend_services/)
# para o diretório de trabalho /app na imagem.
# Isso incluirá sua pasta 'app/',