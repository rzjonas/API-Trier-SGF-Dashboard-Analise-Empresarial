# Painel de Análise Empresarial

## 1. Visão Geral

Este documento descreve de forma técnica a estrutura, requisitos,
dependências e procedimentos necessários para implantação do
**Painel de Análise Empresarial** (Dashboard Trier SGF) em um ambiente
Linux ou Windows.

O sistema opera em duas camadas principais:

-   **Orquestrador (Backend ETL):** Sincronização contínua de dados via
API Trier SGF, processamento de regras de negócio (devoluções, custos,
estoque) e persistência em SQLite.
-   **Aplicação Web (Flask):** Interface visual para análise de Vendas,
Produtos, Estoque e Financeiro, servindo dados processados via API
interna JSON.

------------------------------------------------------------------------

## 2. Estrutura do Projeto

A estrutura mínima necessária para funcionamento:

    API-Trier-SGF-Dashboard-Analise-Empresarial/
    │
    ├── app.py                          # Servidor Web Flask (Frontend + API JSON)
    ├── orquestrador.py                 # Serviço de sincronização de dados (ETL)
    ├── conexao_api_trier_sgf.py        # Módulo de lógica de conexão e tratamento de dados
    ├── config_conexao.py               # (Deve ser criado) Configurações e Tokens
    │
    ├── static/
    │   └── css/
    │       ├── style.css               # Estilos globais
    │       ├── vendas.css              # Estilos específicos de Vendas
    │       ├── produtos_estoque.css    # Estilos de Produtos/Estoque
    │       ├── financeiro_compras.css  # Estilos de Financeiro
    │       └── desempenho.css          # Estilos do Dashboard de Desempenho
    │
    ├── templates/
    │   ├── layout.html                 # Estrutura base (HTML mestre)
    │   ├── vendas.html                 # Página de Análise de Vendas
    │   ├── produtos_estoque.html       # Página de Produtos e Estoque
    │   ├── financeiro_compras.html     # Página de Financeiro
    │   └── desempenho.html             # Página de KPIs e Desempenho
    │
    ├── data/                          # Diretório onde o banco SQLite será salvo
    ├── log.txt                         # Log de execução do orquestrador
    │
    └── .venv/                          # Ambiente virtual Python

------------------------------------------------------------------------

## 3. Dependências do Sistema

Instalar pacotes básicos (exemplo para Ubuntu/Debian):
```
sudo apt update sudo apt install -y python3-venv python3-pip
```

------------------------------------------------------------------------

## 4. Ambiente Virtual Python
```
cd /caminho/para/dashboard_trier python3 -m venv .venv source .venv/bin/activate
```

Instalar dependências do projeto:
```
pip install flask pandas requests
```

*(O sqlite3 já faz parte da biblioteca padrão do Python)*

------------------------------------------------------------------------

## 5. Configuração `config_conexao.py`

O arquivo config_conexao.py **não está incluso no repositório** e deve ser criado na raiz com as seguintes constantes:

``` python
# -*- coding: utf-8 -*-

import os
from datetime import datetime

API_AUTH_TOKEN = "SEU_TOKEN_BEARER_AQUI"

API_BASE_URL = "https://api-sgf-gateway.triersistemas.com.br/sgfpod1"

VENDAS_ENDPOINT = f"{API_BASE_URL}/rest/integracao/venda/obter-todos-v1"
VENDAS_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/venda/obter-alterados-v1"
VENDAS_CANCEL_ENDPOINT = f"{API_BASE_URL}/rest/integracao/venda/cancelamento/obter-todos-v1"
COMPRAS_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/compra/obter-alterados-v1"
VENDEDOR_ENDPOINT = f"{API_BASE_URL}/rest/integracao/vendedor/obter-todos-v1"
PRODUTO_ENDPOINT = f"{API_BASE_URL}/rest/integracao/produto/obter-todos-v1"
PRODUTO_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/produto/obter-alterados-v1"
ESTOQUE_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/estoque/obter-alterados-v1"
FORNECEDOR_ENDPOINT = f"{API_BASE_URL}/rest/integracao/fornecedor/obter-todos-v1"
FORNECEDOR_ALT_ENDPOINT = f"{API_BASE_URL}/rest/integracao/fornecedor/obter-alterados-v1"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, 'data')

DATABASE_FILE = os.path.join(DATA_DIR, 'dados_sgf.sqlite')

os.makedirs(DATA_DIR, exist_ok=True)

HISTORICAL_START_DATE = "2025-10-01"

SALES_FILE_DAYS_INTERVAL = 10

INTERVALO_VENDAS = 10       # A cada 10 minutos, buscará novas vendas.
INTERVALO_COMPRAS = 15      # A cada 15 minutos, buscará novas compras.
INTERVALO_PRODUTOS = 15     # A cada 15 minutos, buscará produtos alterados.
INTERVALO_ESTOQUE = 10      # A cada 10 minutos, buscará alterações de estoque.
INTERVALO_VENDEDORES = 180  # A cada 180 minutos, atualiza a lista de vendedores.
INTERVALO_FORNECEDORES = 20 # A cada 20 minutos, buscará novos fornecedores.

STATE_DIR = os.path.join(DATA_DIR, '.state')

os.makedirs(STATE_DIR, exist_ok=True)
``` 

------------------------------------------------------------------------

## 6. Execução Manual

O sistema requer dois processos rodando simultaneamente.

A. Orquestrador de Dados (ETL):

Responsável por baixar e atualizar os dados no banco SQLite.
``` 
source .venv/bin/activate
python orquestrador.py
``` 
Nota: Na primeira execução, ele fará a carga histórica completa. Isso pode demorar.

B. Servidor Web (Dashboard):

Responsável por servir a interface visual.
``` 
source .venv/bin/activate
python app.py
``` 
Acesse no navegador: http://127.0.0.1:5000

------------------------------------------------------------------------

## 7. Estrutura de Rotas e API
Páginas HTML:

    / ou /analise-vendas: Dashboard de Vendas
    /produtos-estoque: Análise de Curva ABC e Níveis de Estoque
    /financeiro-compras: Gestão de Notas de Entrada
    /desempenho: KPIs e Comparativos Temporais

Endpoints de Dados (JSON):

A interface consome dados processados através destas rotas internas:

GET /api/dados-dashboard
GET /api/dados-produtos-estoque
GET /api/dados-financeiro-compras
GET /api/dados-desempenho

------------------------------------------------------------------------

## 8. Logs e Debug
Logs do Orquestrador:

O script gera um arquivo log.txt local detalhado.

------------------------------------------------------------------------

## 9. Dependências Críticas

Se ausentes, o sistema falhará:

    Arquivo config_conexao.py corretamente configurado.

    Token da API Trier válido.

    Permissão de escrita na pasta dados/ (para o arquivo SQLite).

    Acesso à internet para requisições HTTPS na API externa.

------------------------------------------------------------------------

## 10. Conclusão

Este documento fornece a base técnica para a implantação do Painel de
Análise Empresarial. O sistema foi desenhado para ser resiliente: se o
orquestrador falhar, ele retoma do último checkpoint salvo em estados.
O frontend é desacoplado e lê apenas o banco SQLite, garantindo
performance na navegação.
