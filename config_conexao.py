import os
from datetime import datetime

API_AUTH_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJjb2RfZmlsaWFsIjoiMSIsInNjb3BlIjpbImRyb2dhcmlhIl0sInRva2VuX2ludGVncmFjYW8iOiJ0cnVlIiwiY29kX2Zhcm1hY2lhIjoiMTI5NTgiLCJleHAiOjQxMDI0NTU2MDAsImlhdCI6MTc1MzM4NTM2NywianRpIjoiZjgzZjQ5ZTQtMWMzOS00ZjJkLTg1MWMtNWZhMzJiYmFjZDU2IiwiY29kX3VzdWFyaW8iOiI1MSIsImF1dGhvcml0aWVzIjpbIkFQSV9JTlRFR1JBQ0FPIl19.B8lUvyhFRHStjS922eCifLvY2p7lq8gnmKrct_DEvCg"

# --- Tokens para teste, não excluir ----
## Token cod 16237: "eyJhbGciOiJIUzI1NiJ9.eyJjb2RfZmlsaWFsIjoiMzAyIiwic2NvcGUiOlsiZHJvZ2FyaWEiXSwidG9rZW5faW50ZWdyYWNhbyI6InRydWUiLCJjb2RfZmFybWFjaWEiOiIxNjIzNyIsImV4cCI6NDEwMjQ1NTYwMCwiaWF0IjoxNzI2ODU3NzQwLCJqdGkiOiI4OTU0NzM5MC0zM2NhLTRlZmQtYTk5Ny02NGRjOThiYWI2YmUiLCJjb2RfdXN1YXJpbyI6IjU2MTg1IiwiYXV0aG9yaXRpZXMiOlsiQVBJX0lOVEVHUkFDQU8iXX0.5KdNsv4GVwpjkG2e_C8MKrGoOnQmiAtqTa32rS_J_F0"
## Token Max cod 12958: "eyJhbGciOiJIUzI1NiJ9.eyJjb2RfZmlsaWFsIjoiMSIsInNjb3BlIjpbImRyb2dhcmlhIl0sInRva2VuX2ludGVncmFjYW8iOiJ0cnVlIiwiY29kX2Zhcm1hY2lhIjoiMTI5NTgiLCJleHAiOjQxMDI0NTU2MDAsImlhdCI6MTc1MzM4NTM2NywianRpIjoiZjgzZjQ5ZTQtMWMzOS00ZjJkLTg1MWMtNWZhMzJiYmFjZDU2IiwiY29kX3VzdWFyaW8iOiI1MSIsImF1dGhvcml0aWVzIjpbIkFQSV9JTlRFR1JBQ0FPIl19.B8lUvyhFRHStjS922eCifLvY2p7lq8gnmKrct_DEvCg"

API_BASE_URL = "https://api-sgf-gateway.triersistemas.com.br/sgfpod1"

VENDAS_ENDPOINT = f"{API_BASE_URL}/rest/integracao/venda/obter-todos-v1"
VENDAS_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/venda/obter-alterados-v1"
VENDAS_CANCEL_ENDPOINT = f"{API_BASE_URL}/rest/integracao/venda/cancelamento/obter-todos-v1"

VENDEDOR_ENDPOINT = f"{API_BASE_URL}/rest/integracao/vendedor/obter-todos-v1"

PRODUTO_ENDPOINT = f"{API_BASE_URL}/rest/integracao/produto/obter-todos-v1"
PRODUTO_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/produto/obter-alterados-v1"

ESTOQUE_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/estoque/obter-alterados-v1"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, 'data')

DATABASE_FILE = os.path.join(DATA_DIR, 'dados_sgf.sqlite')

os.makedirs(DATA_DIR, exist_ok=True)

HISTORICAL_START_DATE = "2025-01-01"

SALES_FILE_DAYS_INTERVAL = 10

INTERVALO_VENDAS = 10
INTERVALO_PRODUTOS = 15
INTERVALO_ESTOQUE = 10
INTERVALO_VENDEDORES = 180

STATE_DIR = os.path.join(DATA_DIR, '.state')
os.makedirs(STATE_DIR, exist_ok=True)