import pandas as pd
from flask import Flask, render_template, jsonify, request
import os
import json
import logging
import json

import config_conexao as cfg

from datetime import datetime, timedelta

app = Flask(__name__)

_df_final_cache = None
_df_vendedores_cache = None
_cache_timestamp = None
CACHE_DURATION_MINUTES = 5

def carregar_e_processar_dados():

    global _df_final_cache, _df_vendedores_cache, _cache_timestamp

    if _df_final_cache is not None and _cache_timestamp is not None:
        cache_age = datetime.now() - _cache_timestamp
        if cache_age < timedelta(minutes=CACHE_DURATION_MINUTES):
            return _df_final_cache.copy(), _df_vendedores_cache.copy()

    conn_str = f'sqlite:///{cfg.DATABASE_FILE}'
    df_final = pd.DataFrame()
    df_vendedores = pd.DataFrame()

    try:
        df_final = pd.read_sql_table('vendas_processadas', conn_str)
        df_vendedores = pd.read_sql_table('vendedores', conn_str)
        logging.info(f"Carregados {len(df_final)} registros pré-processados e {len(df_vendedores)} vendedores.")
    except ValueError as e:
        logging.warning(f"Aviso ao carregar dados pré-processados: {e}. Tabela pode não existir ainda.")
    except Exception as e:
        logging.error(f"Erro crítico ao ler do banco de dados: {e}", exc_info=True)
        return pd.DataFrame(), pd.DataFrame()

    _df_final_cache = df_final
    _df_vendedores_cache = df_vendedores
    _cache_timestamp = datetime.now()
    
    return df_final.copy(), df_vendedores.copy()

@app.route('/')
def index_page():
    return render_template('index.html')

@app.route('/analise-vendas')
def analise_vendas_page():
    return render_template('analise_vendas.html')

@app.route('/produtos-estoque')
def produtos_estoque_page():
    return render_template('produtos_estoque.html')

@app.route('/financeiro-compras')
def financeiro_compras_page():
    return render_template('financeiro_compras.html')

@app.route('/desempenho')
def desempenho_page():
    return render_template('desempenho.html')


@app.route('/api/dados-dashboard')
def api_dashboard_data():
    df_final, df_vendedores = carregar_e_processar_dados()

    if df_final.empty:
        return jsonify(sales_data=[], all_sellers=[], vendas_por_pagamento={}, vendas_por_hora={}, vendas_por_vendedor={}, vendas_por_entrega={})

    data_inicio = request.args.get('dataInicio')
    data_fim = request.args.get('dataFim')
    
    if data_inicio and data_fim:
        df_final = df_final[
            (df_final['dataEmissao'] >= data_inicio) & (df_final['dataEmissao'] <= data_fim)
        ]

    vendas_por_pagamento, vendas_por_hora, vendas_por_vendedor, vendas_por_entrega = {}, {}, {}, {}

    if not df_final.empty:
        df_ok = df_final[df_final['status_venda'] == 'OK'].copy()
        if not df_ok.empty:
            vendas_por_pagamento = df_ok.groupby('condicaoPagamento_nome')['valorTotalLiquido'].sum().sort_values(ascending=False).to_dict()
            if 'horaEmissao' in df_ok.columns:
                df_ok['hora'] = df_ok['horaEmissao'].str[:2]
                vendas_hora_agg = df_ok.groupby('hora')['valorTotalLiquido'].sum()
                horas_completas = [str(h).zfill(2) for h in range(24)]
                vendas_hora_agg = vendas_hora_agg.reindex(horas_completas, fill_value=0).sort_index()
                vendas_por_hora = vendas_hora_agg.to_dict()
            vendas_por_vendedor = df_ok.groupby('nomeVendedor')['valorTotalLiquido'].sum().sort_values(ascending=False).to_dict()
            vendas_por_entrega = df_ok.groupby('entrega')['valorTotalLiquido'].sum().sort_values(ascending=False).to_dict()

    sales_data = json.loads(df_final.to_json(orient='records', date_format='iso'))
    all_sellers = json.loads(df_vendedores.to_json(orient='records', date_format='iso'))

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
    df_final, _ = carregar_e_processar_dados()

    if df_final.empty:
        return jsonify({})

    data_inicio = request.args.get('dataInicio')
    data_fim = request.args.get('dataFim')
    
    df_filtrado = df_final.copy()
    if data_inicio and data_fim:
        df_filtrado = df_filtrado[(df_filtrado['dataEmissao'] >= data_inicio) & (df_filtrado['dataEmissao'] <= data_fim)]
    
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
    df_final, _ = carregar_e_processar_dados()
    if df_final.empty: return jsonify({})

    data_inicio_str = request.args.get('dataInicio')
    data_fim_str = request.args.get('dataFim')
    
    df_filtrado = df_final.copy()
    if data_inicio_str and data_fim_str:
        df_filtrado = df_filtrado[(df_filtrado['dataEmissao'] >= data_inicio_str) & (df_filtrado['dataEmissao'] <= data_fim_str)]

    receita_periodo_anterior = 0
    if data_inicio_str and data_fim_str:
        try:
            data_inicio = datetime.strptime(data_inicio_str, '%Y-%m-%d')
            data_fim = datetime.strptime(data_fim_str, '%Y-%m-%d')
            dias_periodo = (data_fim - data_inicio).days + 1
            data_fim_anterior = data_inicio - timedelta(days=1)
            data_inicio_anterior = data_fim_anterior - timedelta(days=dias_periodo - 1)
            data_inicio_anterior_str = data_inicio_anterior.strftime('%Y-%m-%d')
            data_fim_anterior_str = data_fim_anterior.strftime('%Y-%m-%d')
            df_anterior = df_final[(df_final['dataEmissao'] >= data_inicio_anterior_str) & (df_final['dataEmissao'] <= data_fim_anterior_str) & (df_final['status_venda'] == 'OK')]
            receita_periodo_anterior = df_anterior['valorTotalLiquido'].sum()
        except:
            receita_periodo_anterior = 0

    df_ok = df_filtrado[df_filtrado['status_venda'] == 'OK'].copy()
    if df_ok.empty:
        percentual_comparativo = -100 if receita_periodo_anterior > 0 else 0
        return jsonify({'kpis': {'ticket_medio': 0, 'ipt': 0},'evolucao_receita': {'labels': [],'data': []},'top_categorias': {'labels': [], 'data': []},'comparativo': {'atual': 0,'anterior': receita_periodo_anterior,'percentual': percentual_comparativo},'mapa_calor': {},'top_vendedores': {'labels': [], 'data': []}})

    receita_liquida_total = df_ok['valorTotalLiquido'].sum()
    numero_vendas = df_ok['numeroNota'].nunique()
    total_itens = df_ok['quantidadeProdutos'].sum()
    ticket_medio = receita_liquida_total / numero_vendas if numero_vendas > 0 else 0
    itens_por_transacao = total_itens / numero_vendas if numero_vendas > 0 else 0

    percentual_comparativo = ((receita_liquida_total - receita_periodo_anterior) / receita_periodo_anterior) * 100 if receita_periodo_anterior > 0 else (100 if receita_liquida_total > 0 else 0)

    df_ok['dataEmissao_dt'] = pd.to_datetime(df_ok['dataEmissao'])
    receita_por_dia = df_ok.groupby(df_ok['dataEmissao_dt'].dt.strftime('%Y-%m-%d'))['valorTotalLiquido'].sum().sort_index()
    top_5_categorias = df_ok.groupby('nomeGrupo')['valorTotalLiquido'].sum().nlargest(5).sort_values(ascending=True)
    top_5_vendedores = df_ok.groupby('nomeVendedor')['valorTotalLiquido'].sum().nlargest(5).sort_values(ascending=True)

    df_ok['dia_semana'] = df_ok['dataEmissao_dt'].dt.day_name('pt_BR').str.capitalize()
    df_ok['hora'] = pd.to_numeric(df_ok['horaEmissao'].str[:2], errors='coerce').fillna(0).astype(int)
    mapa_calor = df_ok.groupby(['dia_semana', 'hora'])['valorTotalLiquido'].sum().reset_index()
    dias_ordenados = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']
    mapa_calor['dia_semana'] = pd.Categorical(mapa_calor['dia_semana'], categories=dias_ordenados, ordered=True)
    mapa_calor_pivot = mapa_calor.pivot_table(index='dia_semana', columns='hora', values='valorTotalLiquido', fill_value=0, observed=False).reindex(dias_ordenados, fill_value=0)
    for hora in range(24):
        if hora not in mapa_calor_pivot.columns:
            mapa_calor_pivot[hora] = 0
    mapa_calor_pivot = mapa_calor_pivot.reindex(sorted(mapa_calor_pivot.columns), axis=1)

    dados_dashboard = {'kpis': {'ticket_medio': ticket_medio, 'ipt': itens_por_transacao},'evolucao_receita': {'labels': receita_por_dia.index.tolist(),'data': receita_por_dia.values.tolist()},'top_categorias': {'labels': top_5_categorias.index.tolist(),'data': top_5_categorias.values.tolist()},'comparativo': {'atual': receita_liquida_total,'anterior': receita_periodo_anterior,'percentual': percentual_comparativo,'diferenca_valor': receita_liquida_total - receita_periodo_anterior},'mapa_calor': mapa_calor_pivot.to_dict(orient='index'),'top_vendedores': {'labels': top_5_vendedores.index.tolist(),'data': top_5_vendedores.values.tolist()}}
    return jsonify(dados_dashboard)

@app.route('/api/dados-produtos-estoque')
def api_produtos_estoque_data():
    df_final, _ = carregar_e_processar_dados()

    if df_final.empty: return jsonify({})

    df_produtos = pd.DataFrame()
    try:
        df_produtos = pd.read_sql_table('produtos', f'sqlite:///{cfg.DATABASE_FILE}')
        df_produtos['codigo'] = df_produtos['codigo'].astype(str)
    except:
        return jsonify({})

    data_inicio = request.args.get('dataInicio')
    data_fim = request.args.get('dataFim')

    df_vendas_periodo = df_final.copy()
    if data_inicio and data_fim:
        df_vendas_periodo = df_final[(df_final['dataEmissao'] >= data_inicio) & (df_final['dataEmissao'] <= data_fim)]
    
    produtos_ativos = df_produtos[df_produtos['ativo'] == True].shape[0] if 'ativo' in df_produtos.columns else df_produtos.shape[0]

    codigos_produtos_vendidos = df_vendas_periodo['codigoProduto'].unique()
    df_produtos_ativos = df_produtos[df_produtos['ativo'] == True] if 'ativo' in df_produtos.columns else df_produtos
    codigos_produtos_ativos = df_produtos_ativos['codigo'].unique()
    produtos_sem_venda = len(set(codigos_produtos_ativos) - set(codigos_produtos_vendidos))
    
    valor_estoque_custo = 0
    if 'quantidadeEstoque' in df_produtos.columns and ('valorCustoMedio' in df_produtos.columns or 'valorCusto' in df_produtos.columns):
        custo_col = 'valorCustoMedio' if 'valorCustoMedio' in df_produtos.columns else 'valorCusto'
        df_produtos['valor_total_estoque_custo'] = df_produtos['quantidadeEstoque'] * df_produtos[custo_col]
        valor_estoque_custo = df_produtos['valor_total_estoque_custo'].sum()

    estoque_critico = estoque_baixo = estoque_aceitavel = estoque_otimo = 0
    if 'quantidadeEstoque' in df_produtos.columns:
        estoque_critico = df_produtos[df_produtos['quantidadeEstoque'] <= 2].shape[0]
        estoque_baixo = df_produtos[(df_produtos['quantidadeEstoque'] > 2) & (df_produtos['quantidadeEstoque'] <= 5)].shape[0]
        estoque_aceitavel = df_produtos[(df_produtos['quantidadeEstoque'] > 5) & (df_produtos['quantidadeEstoque'] <= 10)].shape[0]
        estoque_otimo = df_produtos[df_produtos['quantidadeEstoque'] > 10].shape[0]

    indicadores = {'produtos_ativos': produtos_ativos, 'produtos_sem_venda': produtos_sem_venda, 'valor_estoque_custo': valor_estoque_custo, 'estoque_critico': estoque_critico, 'estoque_baixo': estoque_baixo, 'estoque_aceitavel': estoque_aceitavel, 'estoque_otimo': estoque_otimo}
    
    df_ok = df_vendas_periodo[df_vendas_periodo['status_venda'] == 'OK'].copy()
    if df_ok.empty:
        return jsonify(indicadores=indicadores, curva_abc={'labels': [], 'data': []}, graficos_top={'top_qtd': {'labels': [], 'data': []}, 'top_receita': {'labels': [], 'data': []}}, tabela_produtos=[])

    receita_por_produto = df_ok.groupby('nome')['valorTotalLiquido'].sum().sort_values(ascending=False)
    receita_total = receita_por_produto.sum()
    percentual_acumulado = (receita_por_produto.cumsum() / receita_total) * 100
    classificacao_abc = percentual_acumulado.apply(lambda p: 'A' if p <= 80 else ('B' if p <= 95 else 'C')).value_counts()
    curva_abc = {'labels': classificacao_abc.index.tolist(), 'data': classificacao_abc.values.tolist()}

    top_10_produtos_qtd = df_ok.groupby('nome')['quantidadeProdutos'].sum().nlargest(10).sort_values(ascending=True)
    top_10_produtos_receita = df_ok.groupby('nome')['valorTotalLiquido'].sum().nlargest(10).sort_values(ascending=True)
    graficos_top = {'top_qtd': {'labels': top_10_produtos_qtd.index.tolist(), 'data': top_10_produtos_qtd.values.tolist()}, 'top_receita': {'labels': top_10_produtos_receita.index.tolist(), 'data': top_10_produtos_receita.values.tolist()}}

    analise_produtos = df_ok.groupby(['codigoProduto', 'nome']).agg(qtd_vendida=('quantidadeProdutos', 'sum'), receita_gerada=('valorTotalLiquido', 'sum'), custo_total=('valorTotalCusto', 'sum')).reset_index()
    analise_produtos['lucro_bruto'] = analise_produtos['receita_gerada'] - analise_produtos['custo_total']
    analise_produtos['margem_percentual'] = (analise_produtos['lucro_bruto'] / analise_produtos['receita_gerada'].replace(0, 1)) * 100
    if 'quantidadeEstoque' in df_produtos.columns:
        tabela_final = pd.merge(analise_produtos, df_produtos[['codigo', 'quantidadeEstoque']], left_on='codigoProduto', right_on='codigo', how='left').fillna(0)
        tabela_final['giro_estoque'] = tabela_final['qtd_vendida'] / tabela_final['quantidadeEstoque'].replace(0, 1)
    else:
        tabela_final = analise_produtos
        tabela_final['quantidadeEstoque'] = 0
        tabela_final['giro_estoque'] = 0
    tabela_json = json.loads(tabela_final.to_json(orient='records'))

    return jsonify(indicadores=indicadores, curva_abc=curva_abc, graficos_top=graficos_top, tabela_produtos=tabela_json)


if __name__ == '__main__':
    os.makedirs(cfg.DATA_DIR, exist_ok=True)
    app.run(debug=True)