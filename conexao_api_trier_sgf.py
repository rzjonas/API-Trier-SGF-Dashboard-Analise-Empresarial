import requests
import pandas as pd
import time
import os
import json
from datetime import datetime, timedelta
import logging
import sqlite3

import config_conexao as cfg

def _converter_objetos_para_json(df: pd.DataFrame) -> pd.DataFrame:
    df_copia = df.copy()
    for col in df_copia.select_dtypes(include=['object']).columns:
        df_copia[col] = df_copia[col].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
        )
    return df_copia

def _get_db_connection_string():
    return f'sqlite:///{cfg.DATABASE_FILE}'

def _escrever_para_db(df: pd.DataFrame, nome_tabela: str, if_exists: str = 'replace'):
    if df.empty:
        logging.info(f"DataFrame para a tabela '{nome_tabela}' está vazio. Nenhuma ação de escrita foi tomada.")
        if if_exists == 'replace':
            try:
                conn = sqlite3.connect(cfg.DATABASE_FILE)
                conn.execute(f"DROP TABLE IF EXISTS {nome_tabela}")
                conn.close()
                logging.info(f"Tabela '{nome_tabela}' existente foi removida pois o novo DataFrame está vazio.")
            except Exception as e:
                logging.error(f"Não foi possível remover a tabela antiga '{nome_tabela}': {e}")
        return
        
    try:
        df_pronto_para_db = _converter_objetos_para_json(df)
        conn_str = _get_db_connection_string()
        df_pronto_para_db.to_sql(nome_tabela, conn_str, if_exists=if_exists, index=False)
        logging.info(f"Sucesso: {len(df)} registros foram escritos na tabela '{nome_tabela}' com a estratégia '{if_exists}'.")
    except Exception as e:
        logging.error(f"Falha ao escrever na tabela '{nome_tabela}' do banco de dados: {e}", exc_info=True)
        raise

def _ler_do_db(nome_tabela: str) -> pd.DataFrame:
    try:
        conn_str = _get_db_connection_string()
        df = pd.read_sql_table(nome_tabela, conn_str)
        logging.info(f"Sucesso: {len(df)} registros lidos da tabela '{nome_tabela}'.")
        return df
    except ValueError:
        logging.warning(f"A tabela '{nome_tabela}' não foi encontrada no banco de dados. Retornando um DataFrame vazio.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Falha ao ler a tabela '{nome_tabela}' do banco de dados: {e}", exc_info=True)
        return pd.DataFrame()

def _salvar_estado(nome_tarefa: str, estado: dict):
    caminho_arquivo = os.path.join(cfg.STATE_DIR, f"{nome_tarefa}.json")
    try:
        with open(caminho_arquivo, 'w') as f:
            json.dump(estado, f, indent=4)
        logging.info(f"Checkpoint salvo para a tarefa '{nome_tarefa}': {estado}")
    except Exception as e:
        logging.error(f"Falha ao salvar o estado para a tarefa '{nome_tarefa}': {e}", exc_info=True)

def _carregar_estado(nome_tarefa: str) -> dict:
    caminho_arquivo = os.path.join(cfg.STATE_DIR, f"{nome_tarefa}.json")
    if os.path.exists(caminho_arquivo):
        try:
            with open(caminho_arquivo, 'r') as f:
                estado = json.load(f)
                logging.info(f"Checkpoint encontrado para '{nome_tarefa}'. Retomando do estado: {estado}")
                return estado
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Falha ao carregar estado para '{nome_tarefa}', recomeçando do zero. Erro: {e}")
    return {}

def _limpar_estado(nome_tarefa: str):
    caminho_arquivo = os.path.join(cfg.STATE_DIR, f"{nome_tarefa}.json")
    if os.path.exists(caminho_arquivo):
        os.remove(caminho_arquivo)
        logging.info(f"Tarefa '{nome_tarefa}' concluída. Checkpoint removido.")

def _concatenar_dfs_com_seguranca(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    if df1.empty: return df2.copy()
    if df2.empty: return df1.copy()
    return pd.concat([df1, df2], ignore_index=True).reindex(columns=df1.columns.union(df2.columns))

def realizar_requisicao_segura(url: str, params: dict = None, headers: dict = None):
    max_ciclos = 2; tentativas_por_ciclo = 5; intervalo_tentativas_s = 10; espera_entre_ciclos_min = 5
    auth_headers = {'Authorization': f'Bearer {cfg.API_AUTH_TOKEN}'}; 
    if headers: auth_headers.update(headers)
    logging.info(f"Iniciando requisição para a URL: {url}"); 
    if params: logging.info(f"Parâmetros: {params}")
    for ciclo in range(1, max_ciclos + 1):
        for tentativa in range(1, tentativas_por_ciclo + 1):
            try:
                response = requests.get(url, params=params, headers=auth_headers, timeout=30)
                response.raise_for_status()
                logging.info("Requisição bem-sucedida!")
                return response.json()
            except requests.exceptions.RequestException as e:
                logging.warning(f"Tentativa {tentativa}/{tentativas_por_ciclo} falhou. Erro: {e}")
                if tentativa < tentativas_por_ciclo: time.sleep(intervalo_tentativas_s)
        if ciclo < max_ciclos:
            logging.warning(f"Ciclo {ciclo} de requisições falhou. Aguardando {espera_entre_ciclos_min} minutos...")
            time.sleep(espera_entre_ciclos_min * 60)
    logging.error(f"Todas as {max_ciclos * tentativas_por_ciclo} tentativas de requisição para {url} falharam.")
    return None

def _buscar_dados_paginados(url: str, params: dict = None, headers: dict = None):
    todos_os_dados = []; primeiro_registro = 0; quantidade_registros = 999
    params_paginacao = params.copy() if params else {}
    while True:
        params_paginacao['primeiroRegistro'] = primeiro_registro
        params_paginacao['quantidadeRegistros'] = quantidade_registros
        pagina_de_dados = realizar_requisicao_segura(url, params=params_paginacao, headers=headers)
        if pagina_de_dados is not None:
            if not pagina_de_dados: break
            todos_os_dados.extend(pagina_de_dados)
            if len(pagina_de_dados) < quantidade_registros: break
            primeiro_registro += quantidade_registros
        else:
            logging.error(f"Falha crítica ao obter página de dados. URL: {url}, Params: {params_paginacao}")
            return None
    return todos_os_dados

def realizar_carga_historica_vendas():
    NOME_TAREFA = 'carga_historica_vendas'; NOME_TABELA = 'vendas'; logging.info(f"\nIniciando carga completa das VENDAS...")
    estado = _carregar_estado(NOME_TAREFA); ultima_data_concluida_str = estado.get('ultima_data_concluida')
    data_inicio_historico = pd.to_datetime(cfg.HISTORICAL_START_DATE); data_inicio_loop = data_inicio_historico
    if ultima_data_concluida_str:
        data_inicio_loop = datetime.strptime(ultima_data_concluida_str, '%Y-%m-%d') + timedelta(days=cfg.SALES_FILE_DAYS_INTERVAL)
    data_fim_loop = datetime.now(); data_atual_periodo = data_inicio_loop
    df_existente = _ler_do_db(NOME_TABELA)
    while data_atual_periodo <= data_fim_loop:
        data_fim_periodo = data_atual_periodo + timedelta(days=cfg.SALES_FILE_DAYS_INTERVAL - 1)
        if data_fim_periodo > data_fim_loop: data_fim_periodo = data_fim_loop
        data_inicio_str = data_atual_periodo.strftime('%Y-%m-%d'); data_fim_str = data_fim_periodo.strftime('%Y-%m-%d')
        logging.info(f"\nProcessando período de {data_inicio_str} a {data_fim_str}")
        params_periodo = {"dataInicial": data_inicio_str, "dataFinal": data_fim_str}
        params_cancel_periodo = {"dataEmissaoInicial": data_inicio_str, "dataEmissaoFinal": data_fim_str}
        dados_vendas = _buscar_dados_paginados(cfg.VENDAS_ALT_ENDPOINT, params=params_periodo)
        dados_cancelados = _buscar_dados_paginados(cfg.VENDAS_CANCEL_ENDPOINT, params=params_cancel_periodo)
        if dados_vendas is None:
            logging.error("Falha ao buscar vendas para o período. A tarefa será retomada na próxima execução.")
            return
        df_vendas_periodo = pd.DataFrame(dados_vendas)
        if not df_vendas_periodo.empty: df_vendas_periodo['status'] = df_vendas_periodo.get('status', pd.Series(dtype='str')).fillna('OK')
        if dados_cancelados:
            df_cancelados = pd.DataFrame(dados_cancelados)
            if not df_cancelados.empty and 'tipoCancelamento' in df_cancelados.columns:
                df_devolvidas = df_cancelados[df_cancelados['tipoCancelamento'] == 'D'].copy()
                if not df_devolvidas.empty:
                    cols_inverter = ['valorTotalCusto', 'valorTotalBruto', 'valorTotalLiquido', 'valorTotal', 'quantidadeProdutos', 'valorDesconto']
                    for col in cols_inverter:
                        if col in df_devolvidas.columns: df_devolvidas[col] = pd.to_numeric(df_devolvidas[col], errors='coerce').fillna(0) * -1
                    df_devolvidas['status'] = 'DEVOLUÇÃO'
                    df_vendas_periodo = _concatenar_dfs_com_seguranca(df_vendas_periodo, df_devolvidas)
                df_excluidas = df_cancelados[df_cancelados['tipoCancelamento'] == 'E'].copy()
                if not df_excluidas.empty:
                    df_excluidas['status'] = 'Excluída'
                    df_vendas_periodo = _concatenar_dfs_com_seguranca(df_vendas_periodo, df_excluidas)
                if not df_vendas_periodo.empty:
                    df_vendas_periodo.drop_duplicates(subset=['numeroNota'], keep='last', inplace=True)
        if not df_vendas_periodo.empty:
            try:
                df_final = pd.concat([df_existente, df_vendas_periodo]).drop_duplicates(subset=['numeroNota'], keep='last')
                _escrever_para_db(df_final, NOME_TABELA, if_exists='replace')
                df_existente = df_final.copy()
            except Exception as e:
                logging.error(f"Falha ao salvar o período no banco de dados. Erro: {e}", exc_info=True)
                return
        else:
            logging.info("Nenhuma venda nova para salvar neste período.")
        _salvar_estado(NOME_TAREFA, {'ultima_data_concluida': data_atual_periodo.strftime('%Y-%m-%d')})
        data_atual_periodo += timedelta(days=cfg.SALES_FILE_DAYS_INTERVAL)
    logging.info(f"Tarefa '{NOME_TAREFA}' concluída com sucesso.")
    _limpar_estado(NOME_TAREFA)


def atualizar_vendas_recentes():
    NOME_TABELA = 'vendas'
    logging.info("\nIniciando atualização de vendas recentes...")
    
    df_existente = _ler_do_db(NOME_TABELA)
    logging.info(f"Encontrados {len(df_existente)} registros existentes na tabela '{NOME_TABELA}'.")
    
    hoje_str = datetime.now().strftime('%Y-%m-%d')

    params_alt = {"dataInicial": hoje_str, "dataFinal": hoje_str}
    dados_alterados = _buscar_dados_paginados(cfg.VENDAS_ALT_ENDPOINT, params=params_alt)
    df_alterados = pd.DataFrame(dados_alterados) if dados_alterados else pd.DataFrame()

    params_cancel = {"dataEmissaoInicial": hoje_str, "dataEmissaoFinal": hoje_str}
    dados_cancelados = _buscar_dados_paginados(cfg.VENDAS_CANCEL_ENDPOINT, params=params_cancel)
    df_cancelados = pd.DataFrame(dados_cancelados) if dados_cancelados else pd.DataFrame()

    if df_alterados.empty and df_cancelados.empty:
        logging.info("Nenhuma venda nova, alterada ou cancelada para processar.")
        return
        
    df_intermediario = df_existente.copy()
    ids_para_remover = set()
    if not df_alterados.empty:
        ids_para_remover.update(df_alterados['numeroNota'].unique())
    if not df_cancelados.empty:
        ids_para_remover.update(df_cancelados['numeroNota'].unique())

    if ids_para_remover:
        df_intermediario = df_intermediario[~df_intermediario['numeroNota'].isin(ids_para_remover)]

    df_novos_e_atualizados = pd.DataFrame()

    if not df_alterados.empty:
        df_alterados['status'] = df_alterados.get('status', pd.Series(dtype='str')).fillna('OK')
        df_novos_e_atualizados = _concatenar_dfs_com_seguranca(df_novos_e_atualizados, df_alterados)
        logging.info(f"{len(df_alterados)} vendas novas/alteradas encontradas.")

    if not df_cancelados.empty:
        logging.info(f"{len(df_cancelados)} registros de cancelamento/devolução encontrados.")
        if 'tipoCancelamento' in df_cancelados.columns:
            df_devolvidas = df_cancelados[df_cancelados['tipoCancelamento'] == 'D'].copy()
            if not df_devolvidas.empty:
                cols_inverter = ['valorTotalCusto', 'valorTotalBruto', 'valorTotalLiquido', 'valorTotal', 'quantidadeProdutos', 'valorDesconto']
                for col in cols_inverter:
                    if col in df_devolvidas.columns: df_devolvidas[col] = pd.to_numeric(df_devolvidas[col], errors='coerce').fillna(0) * -1
                df_devolvidas['status'] = 'DEVOLUÇÃO'
                df_novos_e_atualizados = _concatenar_dfs_com_seguranca(df_novos_e_atualizados, df_devolvidas)

            df_excluidas = df_cancelados[df_cancelados['tipoCancelamento'] == 'E'].copy()
            if not df_excluidas.empty:
                df_excluidas['status'] = 'Excluída'
                df_novos_e_atualizados = _concatenar_dfs_com_seguranca(df_novos_e_atualizados, df_excluidas)

    df_final = _concatenar_dfs_com_seguranca(df_intermediario, df_novos_e_atualizados)
    df_final.drop_duplicates(subset=['numeroNota'], keep='last', inplace=True)
    _escrever_para_db(df_final, NOME_TABELA, if_exists='replace')


def sincronizar_produtos(carga_inicial=False):
    NOME_TABELA = 'produtos'; logging.info("\nIniciando sincronização de produtos...")
    if carga_inicial:
        logging.info("Realizando carga inicial completa de produtos.")
        dados_produtos = _buscar_dados_paginados(cfg.PRODUTO_ENDPOINT)
        if dados_produtos is None:
            logging.error("Falha ao obter dados para a carga inicial de produtos.")
            return
        df_produtos = pd.DataFrame(dados_produtos)
        _escrever_para_db(df_produtos, NOME_TABELA, if_exists='replace')
    else:
        hoje_str = datetime.now().strftime('%Y-%m-%d'); params = {"dataInicial": hoje_str, "dataFinal": hoje_str}
        dados_alterados = _buscar_dados_paginados(cfg.PRODUTO_ALT_ENDPOINT, params=params)
        if not dados_alterados:
            logging.info("Nenhum produto alterado para sincronizar.")
            return
        df_alterados = pd.DataFrame(dados_alterados); df_existente = _ler_do_db(NOME_TABELA)
        df_final = pd.concat([df_existente, df_alterados]).drop_duplicates(subset=['codigo'], keep='last')
        _escrever_para_db(df_final, NOME_TABELA, if_exists='replace')

def sincronizar_vendedores():
    NOME_TABELA = 'vendedores'; logging.info("\nIniciando sincronização de vendedores...")
    dados_vendedores = _buscar_dados_paginados(cfg.VENDEDOR_ENDPOINT)
    if dados_vendedores is not None:
        df_vendedores = pd.DataFrame(dados_vendedores)
        _escrever_para_db(df_vendedores, NOME_TABELA, if_exists='replace')
    else:
        logging.error("Falha ao obter a lista de vendedores da API.")

def processar_e_salvar_dados_analiticos():
    logging.info("Iniciando o reprocessamento dos dados para análise...")
    conn_str = _get_db_connection_string()
    
    try:
        df_vendas = pd.read_sql_table('vendas', conn_str)
        df_vendedores = pd.read_sql_table('vendedores', conn_str)
        df_produtos = pd.read_sql_table('produtos', conn_str)
    except ValueError as e:
        logging.warning(f"Uma ou mais tabelas brutas não existem. Abortando processamento analítico. Erro: {e}")
        return
    except Exception as e:
        logging.error(f"Erro crítico ao ler tabelas brutas: {e}", exc_info=True)
        return

    if df_vendas.empty:
        logging.info("Tabela de vendas está vazia. Nada a processar.")
        _escrever_para_db(pd.DataFrame(), 'vendas_processadas', if_exists='replace')
        return

    def safe_json_loads(s):
        if isinstance(s, str):
            try: return json.loads(s)
            except (json.JSONDecodeError, TypeError): return None
        return s

    for col in ['itens', 'condicaoPagamento']:
        if col in df_vendas.columns: df_vendas[col] = df_vendas[col].apply(safe_json_loads)

    if 'itens' in df_vendas.columns:
        if 'condicaoPagamento' in df_vendas.columns:
            df_vendas['condicaoPagamento_nome'] = df_vendas['condicaoPagamento'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        
        colunas_venda = [col for col in df_vendas.columns if col not in ['itens', 'condicaoPagamento']]
        df_vendas = df_vendas.explode('itens').reset_index(drop=True)
        df_itens_normalized = pd.json_normalize(df_vendas['itens'])
        
        if 'codigoVendedor' in df_itens_normalized.columns:
            df_itens_normalized = df_itens_normalized.drop(columns=['codigoVendedor'])
            
        df_vendas = pd.concat([df_vendas[colunas_venda].reset_index(drop=True), df_itens_normalized.reset_index(drop=True)], axis=1)

    df_vendas['codigoVendedor'] = df_vendas['codigoVendedor'].astype(str)
    df_vendas['codigoProduto'] = df_vendas.get('codigoProduto', pd.Series(dtype='str')).astype(str)
    df_vendas['entrega'] = df_vendas.get('entrega', pd.Series(dtype=bool)).map({True: 'SIM', False: 'NÃO'}).fillna('NÃO')
    df_vendas.rename(columns={'status': 'status_venda'}, inplace=True)
    df_vendas['status_venda'] = df_vendas.get('status_venda', pd.Series(dtype='str')).fillna('OK')

    df_vendedores.rename(columns={'nome': 'nomeVendedor'}, inplace=True)
    df_vendedores['codigo'] = df_vendedores['codigo'].astype(str)
    df_produtos['codigo'] = df_produtos['codigo'].astype(str)

    df_merged = pd.merge(df_vendas, df_vendedores, left_on='codigoVendedor', right_on='codigo', how='left')
    df_merged['nomeVendedor'].fillna('Não encontrado', inplace=True)
    df_final = pd.merge(df_merged, df_produtos, left_on='codigoProduto', right_on='codigo', how='left')
    df_final['nome'].fillna('Produto não encontrado', inplace=True)
    df_final.drop(columns=['codigo_x', 'codigo_y'], errors='ignore', inplace=True)

    colunas_financeiras = ['valorTotalCusto', 'valorTotalBruto', 'valorTotalLiquido', 'quantidadeProdutos']
    for col in colunas_financeiras:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0)
            devolucoes = (df_final['status_venda'] == 'DEVOLUÇÃO')
            df_final.loc[devolucoes, col] = -df_final.loc[devolucoes, col].abs()

    _escrever_para_db(df_final, 'vendas_processadas', if_exists='replace')
    logging.info(f"Sucesso! Tabela 'vendas_processadas' foi atualizada com {len(df_final)} linhas.")

def sincronizar_estoque():
    NOME_TABELA = 'produtos'
    logging.info("\nIniciando sincronização de estoque...")

    hoje_str = datetime.now().strftime('%Y-%m-%d')
    params = {"dataInicial": hoje_str, "dataFinal": hoje_str}
    
    dados_estoque = _buscar_dados_paginados(cfg.ESTOQUE_ALT_ENDPOINT, params=params)

    if not dados_estoque:
        logging.info("Nenhuma alteração de estoque para sincronizar.")
        return

    df_estoque = pd.DataFrame(dados_estoque)
    logging.info(f"Encontrados {len(df_estoque)} registros de estoque alterado.")

    df_produtos = _ler_do_db(NOME_TABELA)
    if df_produtos.empty:
        logging.warning("A tabela de produtos está vazia. Não é possível atualizar o estoque. Execute a carga de produtos primeiro.")
        return

    df_estoque.rename(columns={'codigoProduto': 'codigo'}, inplace=True)
    df_estoque_update = df_estoque[['codigo', 'quantidadeEstoque']]
    
    df_produtos['codigo'] = df_produtos['codigo'].astype(str)
    df_estoque_update['codigo'] = df_estoque_update['codigo'].astype(str)
    
    df_produtos.set_index('codigo', inplace=True)
    df_estoque_update.set_index('codigo', inplace=True)
    
    df_produtos.update(df_estoque_update)
    
    df_produtos.reset_index(inplace=True)
    
    _escrever_para_db(df_produtos, NOME_TABELA, if_exists='replace')