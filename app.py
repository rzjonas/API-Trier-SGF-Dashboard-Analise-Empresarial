# -*- coding: utf-8 -*-

# ==============================================================================
# IMPORTAÇÃO DE BIBLIOTECAS
# ==============================================================================
# Importa as bibliotecas necessárias para o funcionamento da aplicação.
import pandas as pd  # Usado para manipulação e análise de dados em DataFrames.
from flask import Flask, render_template, jsonify, request  # Componentes do Flask para criar o servidor web, renderizar páginas e manipular requisições.
import os  # Usado para interagir com o sistema operacional, como criar diretórios.
import json  # Usado para trabalhar com dados no formato JSON.
import logging  # Para registrar informações, avisos e erros da aplicação.
from datetime import datetime, timedelta  # Para trabalhar com datas e horas, usado no cache e filtros.

# Importa as configurações de conexão, como o caminho do banco de dados.
import config_conexao as cfg

# ==============================================================================
# INICIALIZAÇÃO DA APLICAÇÃO FLASK
# ==============================================================================
# Cria uma instância da aplicação Flask.
app = Flask(__name__)

# ==============================================================================
# CONFIGURAÇÃO DO CACHE EM MEMÓRIA
# ==============================================================================
# Variáveis globais para armazenar os dados em cache e controlar sua validade.
# Isso evita a leitura do banco de dados a cada requisição, melhorando o desempenho.
_df_final_cache = None       # Armazena o DataFrame de vendas processadas.
_df_vendedores_cache = None  # Armazena o DataFrame de vendedores.
_cache_timestamp = None      # Guarda o momento em que o cache de vendas foi criado.

# --- NOVO: Cache para dados de Compras ---
_df_compras_cache = None      # Armazena o DataFrame de compras.
_df_fornecedores_cache = None # Armazena o DataFrame de fornecedores.
_df_produtos_cache = None     # Armazena o DataFrame de produtos.
_compras_cache_timestamp = None # Guarda o momento em que o cache de compras foi criado.

CACHE_DURATION_MINUTES = 5   # Define o tempo de validade do cache em minutos.

# ==============================================================================
# FUNÇÕES DE CARREGAMENTO E PROCESSAMENTO DE DADOS
# ==============================================================================
def carregar_e_processar_dados():
    """
    Carrega os dados de VENDAS e VENDEDORES do banco de dados SQLite.
    Implementa um sistema de cache para evitar leituras repetidas do banco.
    """
    # Torna as variáveis de cache globais acessíveis dentro da função.
    global _df_final_cache, _df_vendedores_cache, _cache_timestamp

    # 1. VERIFICAÇÃO DO CACHE
    if _df_final_cache is not None and _cache_timestamp is not None:
        cache_age = datetime.now() - _cache_timestamp
        if cache_age < timedelta(minutes=CACHE_DURATION_MINUTES):
            return _df_final_cache.copy(), _df_vendedores_cache.copy()

    # 2. CARREGAMENTO DOS DADOS
    conn_str = f'sqlite:///{cfg.DATABASE_FILE}'
    df_final = pd.DataFrame()
    df_vendedores = pd.DataFrame()

    try:
        df_final = pd.read_sql_table('vendas_processadas', conn_str)
        df_vendedores = pd.read_sql_table('vendedores', conn_str)
        logging.info(f"Carregados {len(df_final)} registros de vendas e {len(df_vendedores)} vendedores do banco de dados.")
    except ValueError as e:
        logging.warning(f"Aviso ao carregar dados de vendas: {e}. A tabela pode não existir.")
    except Exception as e:
        logging.error(f"Erro crítico ao ler tabelas de vendas: {e}", exc_info=True)
        return pd.DataFrame(), pd.DataFrame()

    # 3. ATUALIZAÇÃO DO CACHE
    _df_final_cache = df_final
    _df_vendedores_cache = df_vendedores
    _cache_timestamp = datetime.now()
    
    return df_final.copy(), df_vendedores.copy()

# --- NOVO: Função de carregamento de dados para a página de Compras ---
def carregar_dados_compras():
    """
    Carrega os dados de COMPRAS, FORNECEDORES e PRODUTOS do banco de dados SQLite.
    Implementa um sistema de cache dedicado para estas tabelas.
    """
    global _df_compras_cache, _df_fornecedores_cache, _df_produtos_cache, _compras_cache_timestamp

    # 1. VERIFICAÇÃO DO CACHE
    if _df_compras_cache is not None and _compras_cache_timestamp is not None:
        cache_age = datetime.now() - _compras_cache_timestamp
        if cache_age < timedelta(minutes=CACHE_DURATION_MINUTES):
            return _df_compras_cache.copy(), _df_fornecedores_cache.copy(), _df_produtos_cache.copy()

    # 2. CARREGAMENTO DOS DADOS
    conn_str = f'sqlite:///{cfg.DATABASE_FILE}'
    df_compras, df_fornecedores, df_produtos = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    try:
        df_compras = pd.read_sql_table('compras', conn_str)
        df_fornecedores = pd.read_sql_table('fornecedores', conn_str)
        df_produtos = pd.read_sql_table('produtos', conn_str)
        logging.info(f"Carregados {len(df_compras)} registros de compras, {len(df_fornecedores)} fornecedores e {len(df_produtos)} produtos.")
    except ValueError as e:
        logging.warning(f"Aviso ao carregar dados de compras: {e}. Uma das tabelas pode não existir.")
    except Exception as e:
        logging.error(f"Erro crítico ao ler tabelas de compras: {e}", exc_info=True)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 3. ATUALIZAÇÃO DO CACHE
    _df_compras_cache = df_compras
    _df_fornecedores_cache = df_fornecedores
    _df_produtos_cache = df_produtos
    _compras_cache_timestamp = datetime.now()
    
    return df_compras.copy(), df_fornecedores.copy(), df_produtos.copy()

# ==============================================================================
# ROTAS PARA RENDERIZAÇÃO DAS PÁGINAS HTML
# ==============================================================================
# Define as rotas (URLs) que o usuário pode acessar e qual página HTML será mostrada.

@app.route('/')
def dashboard_geral_page():
    """ Rota para a página principal (Dashboard Geral). """
    return render_template('dashboard_geral.html')

@app.route('/analise-vendas')
def analise_vendas_page():
    """ Rota para a página de Análise de Vendas. """
    return render_template('analise_vendas.html')

@app.route('/produtos-estoque')
def produtos_estoque_page():
    """ Rota para a página de Produtos e Estoque. """
    return render_template('produtos_estoque.html')

@app.route('/financeiro-compras')
def financeiro_compras_page():
    """ Rota para a página de Financeiro e Compras (em construção). """
    return render_template('financeiro_compras.html')

@app.route('/desempenho')
def desempenho_page():
    """ Rota para a página de Desempenho (em construção). """
    return render_template('desempenho.html')


# ==============================================================================
# API ENDPOINTS (FORNECIMENTO DE DADOS PARA O FRONT-END)
# ==============================================================================
# As rotas a seguir são APIs que retornam dados em formato JSON para os gráficos
# e tabelas nas páginas HTML, permitindo que o conteúdo seja dinâmico.

@app.route('/api/dados-dashboard')
def api_dashboard_data():
    """
    API para a página 'Análise de Vendas'.
    Fornece dados brutos de vendas, lista de vendedores e agregações para
    os gráficos (vendas por pagamento, hora, vendedor e entrega).
    Aceita os parâmetros 'dataInicio' and 'dataFim' via URL.
    """
    # Carrega os dados, utilizando o cache se possível.
    df_final, df_vendedores = carregar_e_processar_dados()

    # Se não houver dados, retorna uma estrutura JSON vazia.
    if df_final.empty:
        return jsonify(sales_data=[], all_sellers=[], vendas_por_pagamento={}, vendas_por_hora={}, vendas_por_vendedor={}, vendas_por_entrega={})

    # Pega as datas de início e fim dos parâmetros da URL (ex: ?dataInicio=2025-01-01).
    data_inicio = request.args.get('dataInicio')
    data_fim = request.args.get('dataFim')
    
    # Filtra o DataFrame de vendas pelo período de datas, se fornecido.
    if data_inicio and data_fim:
        df_final = df_final[
            (df_final['dataEmissao'] >= data_inicio) & (df_final['dataEmissao'] <= data_fim)
        ]

    # Dicionários para armazenar os dados agregados para os gráficos.
    vendas_por_pagamento, vendas_por_hora, vendas_por_vendedor, vendas_por_entrega = {}, {}, {}, {}

    # Realiza as agregações apenas se houver dados no DataFrame.
    if not df_final.empty:
        # Filtra apenas as vendas com status 'OK' para os cálculos.
        df_ok = df_final[df_final['status_venda'] == 'OK'].copy()
        if not df_ok.empty:
            # Agrupa por condição de pagamento e soma o valor total líquido.
            vendas_por_pagamento = df_ok.groupby('condicaoPagamento_nome')['valorTotalLiquido'].sum().sort_values(ascending=False).to_dict()
            
            # Se a coluna de hora de emissão existir, processa as vendas por hora.
            if 'horaEmissao' in df_ok.columns:
                df_ok['hora'] = df_ok['horaEmissao'].str[:2] # Extrai a hora (os dois primeiros caracteres).
                vendas_hora_agg = df_ok.groupby('hora')['valorTotalLiquido'].sum()
                # Garante que todas as 24 horas do dia estejam presentes, preenchendo com 0 se não houver vendas.
                horas_completas = [str(h).zfill(2) for h in range(24)]
                vendas_hora_agg = vendas_hora_agg.reindex(horas_completas, fill_value=0).sort_index()
                vendas_por_hora = vendas_hora_agg.to_dict()

            # Agrupa por vendedor e por tipo de entrega.
            vendas_por_vendedor = df_ok.groupby('nomeVendedor')['valorTotalLiquido'].sum().sort_values(ascending=False).to_dict()
            vendas_por_entrega = df_ok.groupby('entrega')['valorTotalLiquido'].sum().sort_values(ascending=False).to_dict()

    # Converte os DataFrames para JSON no formato de registros (lista de dicionários).
    sales_data = json.loads(df_final.to_json(orient='records', date_format='iso'))
    all_sellers = json.loads(df_vendedores.to_json(orient='records', date_format='iso'))

    # Retorna todos os dados compilados em um único objeto JSON.
    return jsonify(
        sales_data=sales_data, 
        all_sellers=all_sellers,
        vendas_por_pagamento=vendas_por_pagamento,
        vendas_por_hora=vendas_por_hora,
        vendas_por_vendedor=vendas_por_vendedor,
        vendas_por_entrega=vendas_por_entrega
    )

@app.route('/api/dados-graficos')
def api_graficos_data():
    """
    API para gráficos específicos de produtos (Top 10 por quantidade e receita).
    Obs: Esta API parece não estar sendo utilizada por nenhuma página HTML fornecida.
    """
    df_final, _ = carregar_e_processar_dados()

    if df_final.empty:
        return jsonify({})

    # Filtra os dados por data, se os parâmetros forem fornecidos.
    data_inicio = request.args.get('dataInicio')
    data_fim = request.args.get('dataFim')
    df_filtrado = df_final.copy()
    if data_inicio and data_fim:
        df_filtrado = df_filtrado[(df_filtrado['dataEmissao'] >= data_inicio) & (df_filtrado['dataEmissao'] <= data_fim)]
    
    # Calcula o Top 10 de produtos por quantidade e por receita.
    df_ok = df_filtrado[df_filtrado['status_venda'] == 'OK'].copy()
    top_10_produtos_qtd = df_ok.groupby('nome')['quantidadeProdutos'].sum().nlargest(10)
    top_10_produtos_receita = df_ok.groupby('nome')['valorTotalLiquido'].sum().nlargest(10)
    
    dados_graficos = {
        'top_10_produtos_qtd': top_10_produtos_qtd.to_dict(),
        'top_10_produtos_receita': top_10_produtos_receita.to_dict(),
    }
    
    return jsonify(dados_graficos)

@app.route('/api/dados-dashboard-geral')
def api_dashboard_geral_data():
    """
    API principal para o 'Dashboard Geral'.
    Calcula e retorna todos os KPIs e dados agregados para os gráficos da página inicial,
    incluindo a comparação com o período anterior.
    """
    df_final, _ = carregar_e_processar_dados()
    if df_final.empty: return jsonify({})

    # Obtém e filtra por data.
    data_inicio_str = request.args.get('dataInicio')
    data_fim_str = request.args.get('dataFim')
    df_filtrado = df_final.copy()
    if data_inicio_str and data_fim_str:
        df_filtrado = df_filtrado[(df_filtrado['dataEmissao'] >= data_inicio_str) & (df_filtrado['dataEmissao'] <= data_fim_str)]

    # CÁLCULO DO PERÍODO ANTERIOR PARA COMPARAÇÃO
    receita_periodo_anterior = 0
    if data_inicio_str and data_fim_str:
        try:
            # Converte as strings de data para objetos datetime.
            data_inicio = datetime.strptime(data_inicio_str, '%Y-%m-%d')
            data_fim = datetime.strptime(data_fim_str, '%Y-%m-%d')
            # Calcula a duração do período selecionado.
            dias_periodo = (data_fim - data_inicio).days + 1
            # Define as datas de início e fim do período anterior.
            data_fim_anterior = data_inicio - timedelta(days=1)
            data_inicio_anterior = data_fim_anterior - timedelta(days=dias_periodo - 1)
            # Converte de volta para string para filtrar o DataFrame.
            data_inicio_anterior_str = data_inicio_anterior.strftime('%Y-%m-%d')
            data_fim_anterior_str = data_fim_anterior.strftime('%Y-%m-%d')
            # Filtra o DataFrame original para o período anterior e calcula a receita.
            df_anterior = df_final[(df_final['dataEmissao'] >= data_inicio_anterior_str) & (df_final['dataEmissao'] <= data_fim_anterior_str) & (df_final['status_venda'] == 'OK')]
            receita_periodo_anterior = df_anterior['valorTotalLiquido'].sum()
        except Exception:
            receita_periodo_anterior = 0 # Em caso de erro, o valor é 0.

    # Filtra vendas OK para o período atual.
    df_ok = df_filtrado[df_filtrado['status_venda'] == 'OK'].copy()
    # Se não houver vendas no período, retorna uma estrutura com valores zerados.
    if df_ok.empty:
        percentual_comparativo = -100 if receita_periodo_anterior > 0 else 0
        return jsonify({'kpis': {'ticket_medio': 0, 'ipt': 0},'evolucao_receita': {'labels': [],'data': []},'top_categorias': {'labels': [], 'data': []},'comparativo': {'atual': 0,'anterior': receita_periodo_anterior,'percentual': percentual_comparativo},'mapa_calor': {},'top_vendedores': {'labels': [], 'data': []}})

    # CÁLCULO DOS KPIs (Key Performance Indicators)
    receita_liquida_total = df_ok['valorTotalLiquido'].sum()
    numero_vendas = df_ok['numeroNota'].nunique() # Conta notas únicas para ter o número de transações.
    total_itens = df_ok['quantidadeProdutos'].sum()
    ticket_medio = receita_liquida_total / numero_vendas if numero_vendas > 0 else 0
    itens_por_transacao = total_itens / numero_vendas if numero_vendas > 0 else 0

    # Calcula a variação percentual em relação ao período anterior.
    percentual_comparativo = ((receita_liquida_total - receita_periodo_anterior) / receita_periodo_anterior) * 100 if receita_periodo_anterior > 0 else (100 if receita_liquida_total > 0 else 0)

    # PREPARAÇÃO DOS DADOS PARA OS GRÁFICOS
    df_ok['dataEmissao_dt'] = pd.to_datetime(df_ok['dataEmissao'])
    # Gráfico de evolução da receita: agrupa por dia.
    receita_por_dia = df_ok.groupby(df_ok['dataEmissao_dt'].dt.strftime('%Y-%m-%d'))['valorTotalLiquido'].sum().sort_index()
    # Gráfico de Top 5 Categorias (Grupos de Produtos).
    top_5_categorias = df_ok.groupby('nomeGrupo')['valorTotalLiquido'].sum().nlargest(5).sort_values(ascending=True)
    # Gráfico de Top 5 Vendedores.
    top_5_vendedores = df_ok.groupby('nomeVendedor')['valorTotalLiquido'].sum().nlargest(5).sort_values(ascending=True)

    # PREPARAÇÃO DOS DADOS PARA O MAPA DE CALOR
    df_ok['dia_semana'] = df_ok['dataEmissao_dt'].dt.day_name('pt_BR').str.capitalize()
    df_ok['hora'] = pd.to_numeric(df_ok['horaEmissao'].str[:2], errors='coerce').fillna(0).astype(int)
    mapa_calor = df_ok.groupby(['dia_semana', 'hora'])['valorTotalLiquido'].sum().reset_index()
    # Ordena os dias da semana corretamente.
    dias_ordenados = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']
    mapa_calor['dia_semana'] = pd.Categorical(mapa_calor['dia_semana'], categories=dias_ordenados, ordered=True)
    # Pivota a tabela para ter dias nas linhas e horas nas colunas.
    mapa_calor_pivot = mapa_calor.pivot_table(index='dia_semana', columns='hora', values='valorTotalLiquido', fill_value=0, observed=False).reindex(dias_ordenados, fill_value=0)
    # Garante que todas as 24 colunas de horas existam.
    for hora in range(24):
        if hora not in mapa_calor_pivot.columns:
            mapa_calor_pivot[hora] = 0
    mapa_calor_pivot = mapa_calor_pivot.reindex(sorted(mapa_calor_pivot.columns), axis=1)

    # MONTAGEM DO JSON DE RESPOSTA
    dados_dashboard = {
        'kpis': {'ticket_medio': ticket_medio, 'ipt': itens_por_transacao},
        'evolucao_receita': {'labels': receita_por_dia.index.tolist(),'data': receita_por_dia.values.tolist()},
        'top_categorias': {'labels': top_5_categorias.index.tolist(),'data': top_5_categorias.values.tolist()},
        'comparativo': {'atual': receita_liquida_total,'anterior': receita_periodo_anterior,'percentual': percentual_comparativo,'diferenca_valor': receita_liquida_total - receita_periodo_anterior},
        'mapa_calor': mapa_calor_pivot.to_dict(orient='index'), # Converte o pivot para um dicionário.
        'top_vendedores': {'labels': top_5_vendedores.index.tolist(),'data': top_5_vendedores.values.tolist()}
    }
    return jsonify(dados_dashboard)

@app.route('/api/dados-produtos-estoque')
def api_produtos_estoque_data():
    """
    API para a página de 'Produtos & Estoque'.
    Fornece indicadores de estoque, dados para a curva ABC, gráficos de top produtos
    e uma tabela detalhada de análise de produtos (vendas, custo, lucro, margem, giro).
    """
    df_final, _ = carregar_e_processar_dados()
    if df_final.empty: return jsonify({})

    # Carrega a tabela de produtos, que contém informações de estoque.
    df_produtos = pd.DataFrame()
    try:
        df_produtos = pd.read_sql_table('produtos', f'sqlite:///{cfg.DATABASE_FILE}')
        # Garante que o código do produto seja string para o merge funcionar corretamente.
        df_produtos['codigo'] = df_produtos['codigo'].astype(str)
    except Exception:
        return jsonify({}) # Retorna vazio se a tabela de produtos não existir.

    # Filtra as vendas pelo período selecionado.
    data_inicio = request.args.get('dataInicio')
    data_fim = request.args.get('dataFim')
    df_vendas_periodo = df_final.copy()
    if data_inicio and data_fim:
        df_vendas_periodo = df_final[(df_final['dataEmissao'] >= data_inicio) & (df_final['dataEmissao'] <= data_fim)]
    
    # CÁLCULO DOS INDICADORES DE ESTOQUE (CARDS)
    produtos_ativos = df_produtos[df_produtos['ativo'] == True].shape[0] if 'ativo' in df_produtos.columns else df_produtos.shape[0]

    codigos_produtos_vendidos = df_vendas_periodo['codigoProduto'].unique()
    df_produtos_ativos = df_produtos[df_produtos['ativo'] == True] if 'ativo' in df_produtos.columns else df_produtos
    codigos_produtos_ativos = df_produtos_ativos['codigo'].unique()
    produtos_sem_venda = len(set(codigos_produtos_ativos) - set(codigos_produtos_vendidos))
    
    # Calcula o valor total do estoque com base no custo.
    valor_estoque_custo = 0
    if 'quantidadeEstoque' in df_produtos.columns and ('valorCustoMedio' in df_produtos.columns or 'valorCusto' in df_produtos.columns):
        custo_col = 'valorCustoMedio' if 'valorCustoMedio' in df_produtos.columns else 'valorCusto'
        df_produtos['valor_total_estoque_custo'] = df_produtos['quantidadeEstoque'] * df_produtos[custo_col]
        valor_estoque_custo = df_produtos['valor_total_estoque_custo'].sum()

    # Classifica os produtos por níveis de estoque.
    estoque_critico = estoque_baixo = estoque_aceitavel = estoque_otimo = 0
    if 'quantidadeEstoque' in df_produtos.columns:
        estoque_critico = df_produtos[df_produtos['quantidadeEstoque'] <= 2].shape[0]
        estoque_baixo = df_produtos[(df_produtos['quantidadeEstoque'] > 2) & (df_produtos['quantidadeEstoque'] <= 5)].shape[0]
        estoque_aceitavel = df_produtos[(df_produtos['quantidadeEstoque'] > 5) & (df_produtos['quantidadeEstoque'] <= 10)].shape[0]
        estoque_otimo = df_produtos[df_produtos['quantidadeEstoque'] > 10].shape[0]

    indicadores = {'produtos_ativos': produtos_ativos, 'produtos_sem_venda': produtos_sem_venda, 'valor_estoque_custo': valor_estoque_custo, 'estoque_critico': estoque_critico, 'estoque_baixo': estoque_baixo, 'estoque_aceitavel': estoque_aceitavel, 'estoque_otimo': estoque_otimo}
    
    # Filtra vendas com status OK para a análise de produtos.
    df_ok = df_vendas_periodo[df_vendas_periodo['status_venda'] == 'OK'].copy()
    if df_ok.empty:
        # Se não houver vendas, retorna apenas os indicadores de estoque.
        return jsonify(indicadores=indicadores, curva_abc={'labels': [], 'data': []}, graficos_top={'top_qtd': {'labels': [], 'data': []}, 'top_receita': {'labels': [], 'data': []}}, tabela_produtos=[])

    # CÁLCULO DA CURVA ABC
    receita_por_produto = df_ok.groupby('nome')['valorTotalLiquido'].sum().sort_values(ascending=False)
    receita_total = receita_por_produto.sum()
    percentual_acumulado = (receita_por_produto.cumsum() / receita_total) * 100
    # Classifica os produtos em A (80%), B (15%) e C (5%) da receita.
    classificacao_abc = percentual_acumulado.apply(lambda p: 'A' if p <= 80 else ('B' if p <= 95 else 'C')).value_counts()
    curva_abc = {'labels': classificacao_abc.index.tolist(), 'data': classificacao_abc.values.tolist()}

    # CÁLCULO DOS GRÁFICOS DE TOP 10 PRODUTOS
    top_10_produtos_qtd = df_ok.groupby('nome')['quantidadeProdutos'].sum().nlargest(10).sort_values(ascending=True)
    top_10_produtos_receita = df_ok.groupby('nome')['valorTotalLiquido'].sum().nlargest(10).sort_values(ascending=True)
    graficos_top = {'top_qtd': {'labels': top_10_produtos_qtd.index.tolist(), 'data': top_10_produtos_qtd.values.tolist()}, 'top_receita': {'labels': top_10_produtos_receita.index.tolist(), 'data': top_10_produtos_receita.values.tolist()}}

    # MONTAGEM DA TABELA DE ANÁLISE DE PRODUTOS
    # Agrupa os dados de vendas por produto para calcular métricas.
    analise_produtos = df_ok.groupby(['codigoProduto', 'nome']).agg(qtd_vendida=('quantidadeProdutos', 'sum'), receita_gerada=('valorTotalLiquido', 'sum'), custo_total=('valorTotalCusto', 'sum')).reset_index()
    analise_produtos['lucro_bruto'] = analise_produtos['receita_gerada'] - analise_produtos['custo_total']
    analise_produtos['margem_percentual'] = (analise_produtos['lucro_bruto'] / analise_produtos['receita_gerada'].replace(0, 1)) * 100
    
    # Junta os dados de vendas com os dados de estoque da tabela de produtos.
    if 'quantidadeEstoque' in df_produtos.columns:
        tabela_final = pd.merge(analise_produtos, df_produtos[['codigo', 'quantidadeEstoque']], left_on='codigoProduto', right_on='codigo', how='left').fillna(0)
        # Calcula o giro de estoque simplificado.
        tabela_final['giro_estoque'] = tabela_final['qtd_vendida'] / tabela_final['quantidadeEstoque'].replace(0, 1)
    else:
        # Se não houver dados de estoque, preenche com 0.
        tabela_final = analise_produtos
        tabela_final['quantidadeEstoque'] = 0
        tabela_final['giro_estoque'] = 0
    tabela_json = json.loads(tabela_final.to_json(orient='records'))

    # Retorna todos os dados para a página.
    return jsonify(indicadores=indicadores, curva_abc=curva_abc, graficos_top=graficos_top, tabela_produtos=tabela_json)


# --- NOVO: Endpoint da API para a página de Financeiro & Compras ---
@app.route('/api/dados-financeiro-compras')
def api_financeiro_compras_data():
    """
    API para a página 'Financeiro & Compras'.
    Fornece dados detalhados das notas de compra, enriquecidos com nomes de 
    fornecedores e produtos, prontos para serem exibidos na tabela.
    """
    # 1. CARREGAMENTO DOS DADOS (USANDO A NOVA FUNÇÃO DE CACHE)
    df_compras, df_fornecedores, df_produtos = carregar_dados_compras()

    if df_compras.empty:
        return jsonify(compras_data=[]) # Retorna estrutura vazia se não houver compras

    # 2. FILTRAGEM POR DATA
    data_inicio = request.args.get('dataInicio')
    data_fim = request.args.get('dataFim')
    if data_inicio and data_fim:
        df_compras = df_compras[
            (df_compras['dataEntrada'] >= data_inicio) & (df_compras['dataEntrada'] <= data_fim)
        ]

    if df_compras.empty:
        return jsonify(compras_data=[])

    # 3. PROCESSAMENTO E ENRIQUECIMENTO DOS DADOS
    # Função auxiliar para converter a coluna 'itens' (que é uma string JSON) para uma lista de objetos
    def safe_json_loads(s):
        if isinstance(s, str):
            try:
                return json.loads(s)
            except (json.JSONDecodeError, TypeError):
                return [] # Retorna lista vazia se a string for inválida
        return s if isinstance(s, list) else []

    df_compras['itens'] = df_compras['itens'].apply(safe_json_loads)
    
    # "Explode" o DataFrame para que cada item de uma nota de compra vire uma linha
    df_flat = df_compras.explode('itens').reset_index(drop=True)

    # Normaliza a coluna 'itens' (que agora contém dicionários) em colunas separadas
    df_itens_normalized = pd.json_normalize(df_flat['itens'])
    
    # Junta o DataFrame original (sem a coluna 'itens') com as novas colunas dos itens
    df_flat = pd.concat([df_flat.drop(columns=['itens']), df_itens_normalized], axis=1)

    # 4. JUNÇÃO (MERGE) COM FORNECEDORES E PRODUTOS PARA OBTER NOMES
    # Padroniza os tipos das chaves para a junção
    df_flat['codigoFornecedor'] = df_flat['codigoFornecedor'].astype(str)
    df_flat['codigoProduto'] = df_flat['codigoProduto'].astype(str)
    df_fornecedores['codigo'] = df_fornecedores['codigo'].astype(str)
    df_produtos['codigo'] = df_produtos['codigo'].astype(str)

    # Junta com fornecedores
    df_merged = pd.merge(df_flat, df_fornecedores[['codigo', 'nomeFantasia']], left_on='codigoFornecedor', right_on='codigo', how='left')

    df_merged.rename(columns={'nomeFantasia': 'nomeFornecedor'}, inplace=True)
    df_merged['nomeFornecedor'].fillna('Fornecedor não encontrado', inplace=True)
    
    # Junta com produtos
    df_final = pd.merge(df_merged, df_produtos[['codigo', 'nome']], left_on='codigoProduto', right_on='codigo', how='left')
    df_final.rename(columns={'nome': 'descricaoProduto'}, inplace=True)
    df_final['descricaoProduto'].fillna('Produto não encontrado', inplace=True)
    
    # 5. AJUSTE FINAL DAS COLUNAS PARA O FRONT-END
    # Renomeia e calcula colunas para corresponder ao que o JavaScript espera
    df_final.rename(columns={
        'numeroNotaFiscal': 'numeroNota',
        'quantidadeProdutos': 'quantidade'
    }, inplace=True)
    
    # Calcula o valor total do item
    df_final['valorTotal'] = pd.to_numeric(df_final['quantidade'], errors='coerce').fillna(0) * pd.to_numeric(df_final['valorUnitario'], errors='coerce').fillna(0)

    # Seleciona e ordena as colunas que serão enviadas
    colunas_finais = [
        'numeroNota', 'dataEntrada', 'codigoFornecedor', 'nomeFornecedor', 'valorTotalNota', 'valorTotalProdutos',
        'codigoProduto', 'descricaoProduto', 'quantidade', 'valorUnitario', 'valorTotal'
    ]
    df_final = df_final[[col for col in colunas_finais if col in df_final.columns]]

    # 6. RETORNO DOS DADOS EM FORMATO JSON
    compras_data = json.loads(df_final.to_json(orient='records', date_format='iso'))
    return jsonify(compras_data=compras_data)


# ==============================================================================
# PONTO DE ENTRADA DA APLICAÇÃO
# ==============================================================================
# Este bloco será executado apenas quando o script 'app.py' for rodado diretamente.
if __name__ == '__main__':
    # Cria o diretório de dados se ele não existir.
    os.makedirs(cfg.DATA_DIR, exist_ok=True)
    # Inicia o servidor de desenvolvimento do Flask.
    # debug=True ativa o modo de depuração, que reinicia o servidor a cada alteração no código.
    app.run(debug=True)