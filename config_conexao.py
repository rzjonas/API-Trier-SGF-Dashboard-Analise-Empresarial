# -*- coding: utf-8 -*-

# ==============================================================================
# MÓDULO DE CONFIGURAÇÃO CENTRAL
# ==============================================================================
# Este arquivo centraliza todas as configurações essenciais para a aplicação,
# como tokens de autenticação, URLs de API, caminhos de arquivos e intervalos
# de tempo para sincronização de dados. Manter essas informações em um único
# local facilita a manutenção e evita a necessidade de alterar o código principal
# ao mudar de ambiente (desenvolvimento, produção) ou atualizar credenciais.
# ==============================================================================


# ==============================================================================
# IMPORTAÇÃO DE BIBLIOTECAS
# ==============================================================================
import os  # Biblioteca para interagir com o sistema operacional, usada para manipular caminhos de arquivos e criar diretórios.
from datetime import datetime # Biblioteca para trabalhar com datas e horas (atualmente não usada neste arquivo, mas mantida para possíveis usos futuros).


# ==============================================================================
# CREDENCIAIS E ENDPOINTS DA API
# ==============================================================================

# Token de autenticação Bearer para acessar a API da Trier SGF.
# Este token autoriza as requisições feitas pela aplicação.
API_AUTH_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJjb2RfZmlsaWFsIjoiMzAyIiwic2NvcGUiOlsiZHJvZ2FyaWEiXSwidG9rZW5faW50ZWdyYWNhbyI6InRydWUiLCJjb2RfZmFybWFjaWEiOiIxNjIzNyIsImV4cCI6NDEwMjQ1NTYwMCwiaWF0IjoxNzI2ODU3NzQwLCJqdGkiOiI4OTU0NzM5MC0zM2NhLTRlZmQtYTk5Ny02NGRjOThiYWI2YmUiLCJjb2RfdXN1YXJpbyI6IjU2MTg1IiwiYXV0aG9yaXRpZXMiOlsiQVBJX0lOVEVHUkFDQU8iXX0.5KdNsv4GVwpjkG2e_C8MKrGoOnQmiAtqTa32rS_J_F0"

# --- Tokens para teste, não excluir ----
# Esta seção armazena tokens alternativos
# para testar a aplicação com diferentes lojas ou cenários, sem alterar o token principal.
## Token cod 16237: "eyJhbGciOiJIUzI1NiJ9.eyJjb2RfZmlsaWFsIjoiMzAyIiwic2NvcGUiOlsiZHJvZ2FyaWEiXSwidG9rZW5faW50ZWdyYWNhbyI6InRydWUiLCJjb2RfZmFybWFjaWEiOiIxNjIzNyIsImV4cCI6NDEwMjQ1NTYwMCwiaWF0IjoxNzI2ODU3NzQwLCJqdGkiOiI4OTU0NzM5MC0zM2NhLTRlZmQtYTk5Ny02NGRjOThiYWI2YmUiLCJjb2RfdXN1YXJpbyI6IjU2MTg1IiwiYXV0aG9yaXRpZXMiOlsiQVBJX0lOVEVHUkFDQU8iXX0.5KdNsv4GVwpjkG2e_C8MKrGoOnQmiAtqTa32rS_J_F0"
## Token Max cod 12958: "eyJhbGciOiJIUzI1NiJ9.eyJjb2RfZmlsaWFsIjoiMSIsInNjb3BlIjpbImRyb2dhcmlhIl0sInRva2VuX2ludGVncmFjYW8iOiJ0cnVlIiwiY29kX2Zhcm1hY2lhIjoiMTI5NTgiLCJleHAiOjQxMDI0NTU2MDAsImlhdCI6MTc1MzM4NTM2NywianRpIjoiZjgzZjQ5ZTQtMWMzOS00ZjJkLTg1MWMtNWZhMzJiYmFjZDU2IiwiY29kX3VzdWFyaW8iOiI1MSIsImF1dGhvcml0aWVzIjpbIkFQSV9JTlRFR1JBQ0FPIl19.B8lUvyhFRHStjS922eCifLvY2p7lq8gnmKrct_DEvCg"

# URL base do gateway da API. Todas as chamadas para endpoints específicos partirão desta URL.
API_BASE_URL = "https://api-sgf-gateway.triersistemas.com.br/sgfpod1"

# Endpoints completos para cada recurso da API, construídos a partir da URL base.
# Isso facilita a manutenção, pois se a URL base mudar, só precisamos alterar a variável `API_BASE_URL`.

# Endpoints de Vendas
VENDAS_ENDPOINT = f"{API_BASE_URL}/rest/integracao/venda/obter-todos-v1"          # Para buscar todas as vendas (usado raramente, preferencialmente na carga inicial).
VENDAS_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/venda/obter-alterados-v1"  # Para buscar vendas criadas ou alteradas em um período (mais eficiente para atualizações).
VENDAS_CANCEL_ENDPOINT = f"{API_BASE_URL}/rest/integracao/venda/cancelamento/obter-todos-v1" # Para buscar notas canceladas ou devolvidas.

# Endpoint de Compras
COMPRAS_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/compra/obter-alterados-v1" # NOVO ENDPOINT DE COMPRAS

# Endpoint de Vendedores
VENDEDOR_ENDPOINT = f"{API_BASE_URL}/rest/integracao/vendedor/obter-todos-v1"      # Para buscar a lista completa de vendedores.

# Endpoints de Produtos
PRODUTO_ENDPOINT = f"{API_BASE_URL}/rest/integracao/produto/obter-todos-v1"        # Para a carga inicial de todos os produtos.
PRODUTO_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/produto/obter-alterados-v1" # Para buscar produtos criados ou alterados recentemente.

# Endpoint de Estoque
ESTOQUE_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/estoque/obter-alterados-v1" # Para buscar apenas as movimentações de estoque do dia.

# Endpoints de Fornecedores
FORNECEDOR_ENDPOINT = f"{API_BASE_URL}/rest/integracao/fornecedor/obter-todos-v1"
FORNECEDOR_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/fornecedor/obter-alterados-v1"


# ==============================================================================
# CONFIGURAÇÕES DE DIRETÓRIOS E ARQUIVOS
# ==============================================================================

# Define o caminho absoluto para o diretório onde este arquivo de configuração está localizado.
# Isso garante que os caminhos para outros arquivos e pastas funcionem corretamente,
# independentemente de onde o script for executado.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define o nome do diretório onde os dados, como o banco de dados, serão armazenados.
# `os.path.join` cria um caminho compatível com qualquer sistema operacional (Windows, Linux, etc.).
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Define o caminho completo para o arquivo do banco de dados SQLite.
DATABASE_FILE = os.path.join(DATA_DIR, 'dados_sgf.sqlite')

# Garante que o diretório 'data' exista. Se não existir, ele será criado.
# `exist_ok=True` impede que um erro seja lançado se o diretório já existir.
os.makedirs(DATA_DIR, exist_ok=True)


# ==============================================================================
# CONFIGURAÇÕES DE EXECUÇÃO E AGENDAMENTO
# ==============================================================================

# Data de início para a carga histórica de vendas. O orquestrador buscará todas as
# vendas a partir desta data na primeira execução.
HISTORICAL_START_DATE = "2025-10-01"

# Durante a carga histórica, as vendas são buscadas em lotes de N dias.
# Este valor define o tamanho de cada lote (período). Um valor menor (como 10)
# reduz o consumo de memória e o risco de falhas em requisições muito grandes.
SALES_FILE_DAYS_INTERVAL = 10

# Intervalos de tempo (em minutos) para o orquestrador executar cada tarefa de sincronização
# durante a operação contínua.
INTERVALO_VENDAS = 10       # A cada 10 minutos, buscará novas vendas.
INTERVALO_COMPRAS = 15      # A cada 15 minutos, buscará novas compras.
INTERVALO_PRODUTOS = 15     # A cada 15 minutos, buscará produtos alterados.
INTERVALO_ESTOQUE = 10      # A cada 10 minutos, buscará alterações de estoque.
INTERVALO_VENDEDORES = 180  # A cada 180 minutos (3 horas), atualizará a lista de vendedores.
INTERVALO_FORNECEDORES = 20 # A cada 20 minutos, buscará novos fornecedores.


# ==============================================================================
# CONFIGURAÇÕES DE GERENCIAMENTO DE ESTADO (CHECKPOINT)
# ==============================================================================

# Define o caminho para um diretório oculto '.state' dentro da pasta 'data'.
# Este diretório armazenará arquivos de checkpoint, que salvam o progresso de
# tarefas longas (como a carga histórica), permitindo que elas sejam retomadas
# do ponto onde pararam em caso de interrupção.
STATE_DIR = os.path.join(DATA_DIR, '.state')

# Garante que o diretório de estado exista.
os.makedirs(STATE_DIR, exist_ok=True)