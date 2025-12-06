# -*- coding: utf-8 -*-

# ==============================================================================
# MÓDULO DE CONEXÃO E PROCESSAMENTO DE DADOS DA API
# ==============================================================================
# Este módulo é o coração da extração e transformação de dados (ETL).
# Suas responsabilidades incluem:
# 1. Realizar requisições seguras e paginadas à API da Trier SGF.
# 2. Orquestrar a carga de dados, tanto a histórica (inicial) quanto as atualizações incrementais.
# 3. Armazenar os dados brutos em um banco de dados SQLite local.
# 4. Processar e transformar os dados brutos, enriquecendo-os e salvando-os em uma tabela analítica.
# 5. Gerenciar o estado de tarefas longas (como a carga histórica) para permitir a retomada em caso de falha.
# ==============================================================================


# ==============================================================================
# IMPORTAÇÃO DE BIBLIOTECAS
# ==============================================================================
import requests  # Para realizar chamadas HTTP para a API.
import pandas as pd  # Para manipulação e análise de dados em DataFrames.
import time  # Para pausas estratégicas (ex: em novas tentativas de requisição).
import os    # Para interagir com o sistema operacional (criar pastas, verificar arquivos).
import json  # Para serializar/desserializar objetos Python para o formato JSON.
from datetime import datetime, timedelta  # Para trabalhar com datas e horas.
import logging  # Para registrar eventos, avisos e erros da aplicação.
import sqlite3  # Para conectar e interagir com o banco de dados SQLite.

# Importa as configurações globais (URLs, tokens, caminhos de arquivo).
import config_conexao as cfg


# ==============================================================================
# FUNÇÕES AUXILIARES DE BANCO DE DADOS E MANIPULAÇÃO DE DADOS
# ==============================================================================

def _converter_objetos_para_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas de um DataFrame que contêm objetos Python (listas/dicionários)
    em strings JSON. O SQLite não suporta tipos de dados complexos nativamente,
    então essa conversão é necessária para armazená-los corretamente.

    Args:
        df (pd.DataFrame): O DataFrame a ser processado.

    Returns:
        pd.DataFrame: Uma cópia do DataFrame com as colunas de objeto convertidas para JSON.
    """
    df_copia = df.copy()
    # Itera sobre todas as colunas do tipo 'object' (que podem ser strings, listas, dicts, etc.).
    for col in df_copia.select_dtypes(include=['object']).columns:
        # Aplica a conversão para JSON apenas se o valor for um dicionário ou lista.
        df_copia[col] = df_copia[col].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
        )
    return df_copia

def _get_db_connection_string() -> str:
    """
    Retorna a string de conexão formatada para o banco de dados SQLite,
    utilizando o caminho definido no arquivo de configuração.

    Returns:
        str: A string de conexão no formato 'sqlite:///caminho/para/o/banco.sqlite'.
    """
    return f'sqlite:///{cfg.DATABASE_FILE}'

def _escrever_para_db(df: pd.DataFrame, nome_tabela: str, if_exists: str = 'replace'):
    """
    Escreve um DataFrame em uma tabela do banco de dados SQLite.

    Args:
        df (pd.DataFrame): O DataFrame a ser salvo.
        nome_tabela (str): O nome da tabela de destino no banco.
        if_exists (str): Estratégia a ser usada se a tabela já existir.
                         'replace': Apaga a tabela antiga e cria uma nova.
                         'append': Adiciona os dados ao final da tabela existente.
    """
    # Se o DataFrame estiver vazio, não há nada para escrever.
    if df.empty:
        logging.info(f"DataFrame para a tabela '{nome_tabela}' está vazio. Nenhuma ação de escrita foi tomada.")
        # Se a estratégia for 'replace', remove a tabela antiga para não deixar dados obsoletos.
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
        # Prepara o DataFrame para o banco, convertendo objetos em JSON.
        df_pronto_para_db = _converter_objetos_para_json(df)
        conn_str = _get_db_connection_string()
        # Usa a função to_sql do pandas para escrever os dados no banco.
        df_pronto_para_db.to_sql(nome_tabela, conn_str, if_exists=if_exists, index=False)
        logging.info(f"Sucesso: {len(df)} registros foram escritos na tabela '{nome_tabela}' com a estratégia '{if_exists}'.")
    except Exception as e:
        logging.error(f"Falha ao escrever na tabela '{nome_tabela}' do banco de dados: {e}", exc_info=True)
        raise

def _ler_do_db(nome_tabela: str) -> pd.DataFrame:
    """
    Lê uma tabela completa do banco de dados SQLite e a retorna como um DataFrame.

    Args:
        nome_tabela (str): O nome da tabela a ser lida.

    Returns:
        pd.DataFrame: O DataFrame com os dados da tabela. Retorna um DataFrame vazio
                      se a tabela não existir ou se ocorrer um erro.
    """
    try:
        conn_str = _get_db_connection_string()
        # Usa a função read_sql_table do pandas para ler a tabela.
        df = pd.read_sql_table(nome_tabela, conn_str)
        logging.info(f"Sucesso: {len(df)} registros lidos da tabela '{nome_tabela}'.")
        return df
    except ValueError:
        # Ocorre quando a tabela não existe no banco.
        logging.warning(f"A tabela '{nome_tabela}' não foi encontrada no banco de dados. Retornando um DataFrame vazio.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Falha ao ler a tabela '{nome_tabela}' do banco de dados: {e}", exc_info=True)
        return pd.DataFrame()

# ==============================================================================
# FUNÇÕES DE GERENCIAMENTO DE ESTADO (CHECKPOINT)
# ==============================================================================
# Estas funções permitem que tarefas longas (como a carga histórica) sejam interrompidas
# e retomadas do ponto onde pararam, salvando o progresso em um arquivo.

def _salvar_estado(nome_tarefa: str, estado: dict):
    """
    Salva o estado atual de uma tarefa em um arquivo JSON (checkpoint).

    Args:
        nome_tarefa (str): Um identificador único para a tarefa (ex: 'carga_historica_vendas').
        estado (dict): Um dicionário contendo o estado a ser salvo (ex: {'ultima_data_concluida': '2025-01-10'}).
    """
    caminho_arquivo = os.path.join(cfg.STATE_DIR, f"{nome_tarefa}.json")
    try:
        with open(caminho_arquivo, 'w') as f:
            json.dump(estado, f, indent=4)
        logging.info(f"Checkpoint salvo para a tarefa '{nome_tarefa}': {estado}")
    except Exception as e:
        logging.error(f"Falha ao salvar o estado para a tarefa '{nome_tarefa}': {e}", exc_info=True)

def _carregar_estado(nome_tarefa: str) -> dict:
    """
    Carrega o último estado salvo de uma tarefa a partir de seu arquivo de checkpoint.

    Args:
        nome_tarefa (str): O identificador da tarefa.

    Returns:
        dict: O dicionário com o estado salvo. Retorna um dicionário vazio se
              nenhum estado for encontrado ou se houver erro na leitura.
    """
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
    """
    Remove o arquivo de checkpoint de uma tarefa, geralmente após sua conclusão bem-sucedida.

    Args:
        nome_tarefa (str): O identificador da tarefa.
    """
    caminho_arquivo = os.path.join(cfg.STATE_DIR, f"{nome_tarefa}.json")
    if os.path.exists(caminho_arquivo):
        os.remove(caminho_arquivo)
        logging.info(f"Tarefa '{nome_tarefa}' concluída. Checkpoint removido.")

def _concatenar_dfs_com_seguranca(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Concatena dois DataFrames de forma segura, tratando casos onde um ou ambos
    podem estar vazios e alinhando as colunas.

    Returns:
        pd.DataFrame: O DataFrame resultante da concatenação.
    """
    if df1.empty: return df2.copy()
    if df2.empty: return df1.copy()
    # Concatena e depois realinha as colunas para garantir a compatibilidade.
    return pd.concat([df1, df2], ignore_index=True).reindex(columns=df1.columns.union(df2.columns))


# ==============================================================================
# FUNÇÕES DE COMUNICAÇÃO COM A API
# ==============================================================================

def realizar_requisicao_segura(url: str, params: dict = None, headers: dict = None):
    """
    Realiza uma requisição GET para a API de forma robusta, com um mecanismo
    de retentativas em caso de falha.

    Args:
        url (str): A URL do endpoint da API.
        params (dict, optional): Parâmetros a serem enviados na URL. Defaults to None.
        headers (dict, optional): Cabeçalhos HTTP adicionais. Defaults to None.

    Returns:
        dict or None: A resposta da API em formato JSON, ou None se todas as
                      tentativas de requisição falharem.
    """
    # Configurações de retentativa.
    max_ciclos = 2
    tentativas_por_ciclo = 5
    intervalo_tentativas_s = 10
    espera_entre_ciclos_min = 5
    
    # Adiciona o token de autorização aos cabeçalhos.
    auth_headers = {'Authorization': f'Bearer {cfg.API_AUTH_TOKEN}'}
    if headers: auth_headers.update(headers)
    
    logging.info(f"Iniciando requisição para a URL: {url}")
    if params: logging.info(f"Parâmetros: {params}")

    # Loop de ciclos de retentativa.
    for ciclo in range(1, max_ciclos + 1):
        # Loop de tentativas dentro de um ciclo.
        for tentativa in range(1, tentativas_por_ciclo + 1):
            try:
                response = requests.get(url, params=params, headers=auth_headers, timeout=30)
                response.raise_for_status()  # Lança um erro para status HTTP 4xx ou 5xx.
                logging.info("Requisição bem-sucedida!")
                return response.json()
            except requests.exceptions.RequestException as e:
                logging.warning(f"Tentativa {tentativa}/{tentativas_por_ciclo} falhou. Erro: {e}")
                if tentativa < tentativas_por_ciclo: time.sleep(intervalo_tentativas_s)
        
        # Se um ciclo inteiro falhar, espera um tempo maior antes de iniciar o próximo.
        if ciclo < max_ciclos:
            logging.warning(f"Ciclo {ciclo} de requisições falhou. Aguardando {espera_entre_ciclos_min} minutos...")
            time.sleep(espera_entre_ciclos_min * 60)
            
    logging.error(f"Todas as {max_ciclos * tentativas_por_ciclo} tentativas de requisição para {url} falharam.")
    return None

def _buscar_dados_paginados(url: str, params: dict = None, headers: dict = None):
    """
    Busca todos os dados de um endpoint da API que utiliza paginação.
    A função realiza requisições sequenciais, incrementando a página, até que
    todos os dados tenham sido coletados.

    Args:
        url (str): A URL base do endpoint.
        params (dict, optional): Parâmetros de filtro (ex: data). Defaults to None.
        headers (dict, optional): Cabeçalhos adicionais. Defaults to None.

    Returns:
        list or None: Uma lista contendo todos os registros coletados, ou None
                      em caso de falha crítica.
    """
    todos_os_dados = []
    primeiro_registro = 0
    quantidade_registros = 999  # Tamanho da página.
    params_paginacao = params.copy() if params else {}
    
    while True:
        # Adiciona os parâmetros de paginação à requisição.
        params_paginacao['primeiroRegistro'] = primeiro_registro
        params_paginacao['quantidadeRegistros'] = quantidade_registros
        
        pagina_de_dados = realizar_requisicao_segura(url, params=params_paginacao, headers=headers)
        
        if pagina_de_dados is not None:
            if not pagina_de_dados: break  # Se a página vier vazia, significa que não há mais dados.
            
            todos_os_dados.extend(pagina_de_dados)
            
            # Se a quantidade de dados recebidos for menor que o tamanho da página, é a última página.
            if len(pagina_de_dados) < quantidade_registros: break
            
            # Prepara para a próxima página.
            primeiro_registro += quantidade_registros
        else:
            # Em caso de falha na requisição segura, aborta a busca paginada.
            logging.error(f"Falha crítica ao obter página de dados. URL: {url}, Params: {params_paginacao}")
            return None
            
    return todos_os_dados


# ==============================================================================
# FUNÇÕES DE SINCRONIZAÇÃO DE DADOS (ORQUESTRAÇÃO)
# ==============================================================================

def realizar_carga_historica_vendas():
    """
    Executa a carga completa do histórico de vendas desde a data definida em
    `HISTORICAL_START_DATE`. A função processa os dados em lotes (períodos de
    `SALES_FILE_DAYS_INTERVAL` dias) e utiliza o sistema de checkpoint para
    ser retomada em caso de falha.
    """
    NOME_TAREFA = 'carga_historica_vendas'
    NOME_TABELA = 'vendas'
    logging.info(f"\nIniciando carga completa das VENDAS...")
    
    # Carrega o último estado salvo para saber de onde continuar.
    estado = _carregar_estado(NOME_TAREFA)
    ultima_data_concluida_str = estado.get('ultima_data_concluida')
    
    # Define as datas de início e fim do processo.
    data_inicio_historico = pd.to_datetime(cfg.HISTORICAL_START_DATE)
    data_inicio_loop = data_inicio_historico
    if ultima_data_concluida_str:
        # Se há um checkpoint, começa do dia seguinte ao último concluído.
        data_inicio_loop = datetime.strptime(ultima_data_concluida_str, '%Y-%m-%d') + timedelta(days=cfg.SALES_FILE_DAYS_INTERVAL)
    
    data_fim_loop = datetime.now()
    data_atual_periodo = data_inicio_loop
    
    df_existente = _ler_do_db(NOME_TABELA) # Carrega os dados já salvos no banco.

    # Loop que itera sobre os períodos de tempo, do início ao fim.
    while data_atual_periodo <= data_fim_loop:
        data_fim_periodo = data_atual_periodo + timedelta(days=cfg.SALES_FILE_DAYS_INTERVAL - 1)
        if data_fim_periodo > data_fim_loop: data_fim_periodo = data_fim_loop
        
        data_inicio_str = data_atual_periodo.strftime('%Y-%m-%d')
        data_fim_str = data_fim_periodo.strftime('%Y-%m-%d')
        logging.info(f"\nProcessando período de {data_inicio_str} a {data_fim_str}")
        
        # Parâmetros para buscar vendas e cancelamentos no período.
        params_periodo = {"dataInicial": data_inicio_str, "dataFinal": data_fim_str}
        params_cancel_periodo = {"dataEmissaoInicial": data_inicio_str, "dataEmissaoFinal": data_fim_str}
        
        # Busca os dados na API.
        dados_vendas = _buscar_dados_paginados(cfg.VENDAS_ALT_ENDPOINT, params=params_periodo)
        dados_cancelados = _buscar_dados_paginados(cfg.VENDAS_CANCEL_ENDPOINT, params=params_cancel_periodo)
        
        if dados_vendas is None:
            logging.error("Falha ao buscar vendas para o período. A tarefa será retomada na próxima execução.")
            return # Aborta a execução para tentar novamente mais tarde.
            
        df_vendas_periodo = pd.DataFrame(dados_vendas)
        if not df_vendas_periodo.empty: df_vendas_periodo['status'] = df_vendas_periodo.get('status', pd.Series(dtype='str')).fillna('OK')
        
        # Processa os dados de cancelamento, separando devoluções de exclusões.
        if dados_cancelados:
            df_cancelados = pd.DataFrame(dados_cancelados)
            if not df_cancelados.empty and 'tipoCancelamento' in df_cancelados.columns:
                # Trata devoluções ('D'): inverte os sinais dos valores financeiros.
                df_devolvidas = df_cancelados[df_cancelados['tipoCancelamento'] == 'D'].copy()
                if not df_devolvidas.empty:
                    cols_inverter = ['valorTotalCusto', 'valorTotalBruto', 'valorTotalLiquido', 'valorTotal', 'quantidadeProdutos', 'valorDesconto']
                    for col in cols_inverter:
                        if col in df_devolvidas.columns: df_devolvidas[col] = pd.to_numeric(df_devolvidas[col], errors='coerce').fillna(0) * -1
                    df_devolvidas['status'] = 'DEVOLUÇÃO'
                    df_vendas_periodo = _concatenar_dfs_com_seguranca(df_vendas_periodo, df_devolvidas)

                # Trata exclusões ('E'): apenas marca o status.
                df_excluidas = df_cancelados[df_cancelados['tipoCancelamento'] == 'E'].copy()
                if not df_excluidas.empty:
                    df_excluidas['status'] = 'Excluída'
                    df_vendas_periodo = _concatenar_dfs_com_seguranca(df_vendas_periodo, df_excluidas)
                
                # Garante que, se uma nota foi alterada e depois cancelada, a versão do cancelamento prevaleça.
                if not df_vendas_periodo.empty:
                    df_vendas_periodo.drop_duplicates(subset=['numeroNota'], keep='last', inplace=True)
                    
        if not df_vendas_periodo.empty:
            try:
                # Junta os dados novos com os já existentes e remove duplicatas.
                df_final = pd.concat([df_existente, df_vendas_periodo]).drop_duplicates(subset=['numeroNota'], keep='last')
                _escrever_para_db(df_final, NOME_TABELA, if_exists='replace')
                df_existente = df_final.copy() # Atualiza o DataFrame em memória para a próxima iteração.
            except Exception as e:
                logging.error(f"Falha ao salvar o período no banco de dados. Erro: {e}", exc_info=True)
                return
        else:
            logging.info("Nenhuma venda nova para salvar neste período.")
            
        # Salva o progresso no arquivo de checkpoint.
        _salvar_estado(NOME_TAREFA, {'ultima_data_concluida': data_atual_periodo.strftime('%Y-%m-%d')})
        # Avança para o próximo período.
        data_atual_periodo += timedelta(days=cfg.SALES_FILE_DAYS_INTERVAL)
        
    logging.info(f"Tarefa '{NOME_TAREFA}' concluída com sucesso.")
    _limpar_estado(NOME_TAREFA) # Remove o checkpoint ao final.


def atualizar_vendas_recentes():
    """
    Busca apenas as vendas do dia corrente que foram criadas, alteradas ou canceladas.
    Esta função é mais leve e rápida que a carga histórica, ideal para atualizações frequentes.
    """
    NOME_TABELA = 'vendas'
    logging.info("\nIniciando atualização de vendas recentes...")
    
    # Carrega todos os dados de vendas existentes.
    df_existente = _ler_do_db(NOME_TABELA)
    logging.info(f"Encontrados {len(df_existente)} registros existentes na tabela '{NOME_TABELA}'.")
    
    hoje_str = datetime.now().strftime('%Y-%m-%d')

    # Busca vendas e cancelamentos apenas para a data de hoje.
    params_alt = {"dataInicial": hoje_str, "dataFinal": hoje_str}
    dados_alterados = _buscar_dados_paginados(cfg.VENDAS_ALT_ENDPOINT, params=params_alt)
    df_alterados = pd.DataFrame(dados_alterados) if dados_alterados else pd.DataFrame()

    params_cancel = {"dataEmissaoInicial": hoje_str, "dataEmissaoFinal": hoje_str}
    dados_cancelados = _buscar_dados_paginados(cfg.VENDAS_CANCEL_ENDPOINT, params=params_cancel)
    df_cancelados = pd.DataFrame(dados_cancelados) if dados_cancelados else pd.DataFrame()

    if df_alterados.empty and df_cancelados.empty:
        logging.info("Nenhuma venda nova, alterada ou cancelada para processar.")
        return
        
    # Lógica de atualização: remove as versões antigas das notas que foram alteradas/canceladas
    # e depois adiciona as novas versões.
    df_intermediario = df_existente.copy()
    ids_para_remover = set()
    if not df_alterados.empty:
        ids_para_remover.update(df_alterados['numeroNota'].unique())
    if not df_cancelados.empty:
        ids_para_remover.update(df_cancelados['numeroNota'].unique())

    if ids_para_remover:
        df_intermediario = df_intermediario[~df_intermediario['numeroNota'].isin(ids_para_remover)]

    # Processa os novos dados (alterados e cancelados).
    df_novos_e_atualizados = pd.DataFrame()

    if not df_alterados.empty:
        df_alterados['status'] = df_alterados.get('status', pd.Series(dtype='str')).fillna('OK')
        df_novos_e_atualizados = _concatenar_dfs_com_seguranca(df_novos_e_atualizados, df_alterados)
        logging.info(f"{len(df_alterados)} vendas novas/alteradas encontradas.")

    if not df_cancelados.empty:
        logging.info(f"{len(df_cancelados)} registros de cancelamento/devolução encontrados.")
        if 'tipoCancelamento' in df_cancelados.columns:
            # Trata devoluções.
            df_devolvidas = df_cancelados[df_cancelados['tipoCancelamento'] == 'D'].copy()
            if not df_devolvidas.empty:
                cols_inverter = ['valorTotalCusto', 'valorTotalBruto', 'valorTotalLiquido', 'valorTotal', 'quantidadeProdutos', 'valorDesconto']
                for col in cols_inverter:
                    if col in df_devolvidas.columns: df_devolvidas[col] = pd.to_numeric(df_devolvidas[col], errors='coerce').fillna(0) * -1
                df_devolvidas['status'] = 'DEVOLUÇÃO'
                df_novos_e_atualizados = _concatenar_dfs_com_seguranca(df_novos_e_atualizados, df_devolvidas)
            # Trata exclusões.
            df_excluidas = df_cancelados[df_cancelados['tipoCancelamento'] == 'E'].copy()
            if not df_excluidas.empty:
                df_excluidas['status'] = 'Excluída'
                df_novos_e_atualizados = _concatenar_dfs_com_seguranca(df_novos_e_atualizados, df_excluidas)

    # Junta o DataFrame original (sem as notas alteradas) com as novas versões.
    df_final = _concatenar_dfs_com_seguranca(df_intermediario, df_novos_e_atualizados)
    df_final.drop_duplicates(subset=['numeroNota'], keep='last', inplace=True)
    _escrever_para_db(df_final, NOME_TABELA, if_exists='replace')


def sincronizar_produtos(carga_inicial=False):
    """
    Sincroniza os dados de produtos. Pode realizar uma carga completa ou apenas
    buscar os produtos alterados no dia.

    Args:
        carga_inicial (bool): Se True, busca todos os produtos. Se False, busca
                              apenas os alterados no dia.
    """
    NOME_TABELA = 'produtos'
    logging.info("\nIniciando sincronização de produtos...")
    if carga_inicial:
        logging.info("Realizando carga inicial completa de produtos.")
        dados_produtos = _buscar_dados_paginados(cfg.PRODUTO_ENDPOINT)
        if dados_produtos is None:
            logging.error("Falha ao obter dados para a carga inicial de produtos.")
            return
        df_produtos = pd.DataFrame(dados_produtos)
        _escrever_para_db(df_produtos, NOME_TABELA, if_exists='replace')
    else:
        # Busca apenas produtos alterados hoje.
        hoje_str = datetime.now().strftime('%Y-%m-%d')
        params = {"dataInicial": hoje_str, "dataFinal": hoje_str}
        dados_alterados = _buscar_dados_paginados(cfg.PRODUTO_ALT_ENDPOINT, params=params)
        if not dados_alterados:
            logging.info("Nenhum produto alterado para sincronizar.")
            return
        # Lógica de atualização: junta os produtos existentes com os alterados,
        # mantendo a versão mais recente em caso de duplicatas.
        df_alterados = pd.DataFrame(dados_alterados)
        df_existente = _ler_do_db(NOME_TABELA)
        df_final = pd.concat([df_existente, df_alterados]).drop_duplicates(subset=['codigo'], keep='last')
        _escrever_para_db(df_final, NOME_TABELA, if_exists='replace')

def sincronizar_vendedores():
    """
    Busca a lista completa de vendedores da API e substitui a tabela local.
    Esta operação é geralmente leve e pode ser feita por completo.
    """
    NOME_TABELA = 'vendedores'
    logging.info("\nIniciando sincronização de vendedores...")
    dados_vendedores = _buscar_dados_paginados(cfg.VENDEDOR_ENDPOINT)
    if dados_vendedores is not None:
        df_vendedores = pd.DataFrame(dados_vendedores)
        _escrever_para_db(df_vendedores, NOME_TABELA, if_exists='replace')
    else:
        logging.error("Falha ao obter a lista de vendedores da API.")

def processar_e_salvar_dados_analiticos():
    """
    Esta é a etapa de "Transformação" do ETL. Ela lê as tabelas de dados brutos
    ('vendas', 'vendedores', 'produtos'), realiza junções (joins), limpezas e
    cálculos para criar uma única tabela enriquecida ('vendas_processadas'),
    otimizada para as consultas do dashboard.
    """
    logging.info("Iniciando o reprocessamento dos dados para análise...")
    conn_str = _get_db_connection_string()
    
    try:
        # Carrega as tabelas de dados brutos.
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

    # Função para desserializar colunas JSON de volta para objetos Python.
    def safe_json_loads(s):
        if isinstance(s, str):
            try: return json.loads(s)
            except (json.JSONDecodeError, TypeError): return None
        return s

    # Aplica a desserialização nas colunas que contêm JSON.
    for col in ['itens', 'condicaoPagamento']:
        if col in df_vendas.columns: df_vendas[col] = df_vendas[col].apply(safe_json_loads)

    # "Explode" a tabela de vendas: cada item de uma venda se torna uma linha separada.
    if 'itens' in df_vendas.columns:
        if 'condicaoPagamento' in df_vendas.columns:
            df_vendas['condicaoPagamento_nome'] = df_vendas['condicaoPagamento'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        
        colunas_venda = [col for col in df_vendas.columns if col not in ['itens', 'condicaoPagamento']]
        df_vendas = df_vendas.explode('itens').reset_index(drop=True)
        df_itens_normalized = pd.json_normalize(df_vendas['itens'])
        
        if 'codigoVendedor' in df_itens_normalized.columns:
            df_itens_normalized = df_itens_normalized.drop(columns=['codigoVendedor'])
            
        df_vendas = pd.concat([df_vendas[colunas_venda].reset_index(drop=True), df_itens_normalized.reset_index(drop=True)], axis=1)

    # Padroniza os tipos de dados para garantir a consistência nas junções.
    df_vendas['codigoVendedor'] = df_vendas['codigoVendedor'].astype(str)
    df_vendas['codigoProduto'] = df_vendas.get('codigoProduto', pd.Series(dtype='str')).astype(str)
    df_vendas['entrega'] = df_vendas.get('entrega', pd.Series(dtype=bool)).map({True: 'SIM', False: 'NÃO'}).fillna('NÃO')
    df_vendas.rename(columns={'status': 'status_venda'}, inplace=True)
    df_vendas['status_venda'] = df_vendas.get('status_venda', pd.Series(dtype='str')).fillna('OK')

    df_vendedores.rename(columns={'nome': 'nomeVendedor'}, inplace=True)
    df_vendedores['codigo'] = df_vendedores['codigo'].astype(str)
    
    # <<< INÍCIO DA CORREÇÃO >>>
    # Prepara o DataFrame de produtos para a junção, selecionando as colunas de interesse.
    if not df_produtos.empty:
        # NOVO: Incluímos 'nome' e o renomeamos para 'nome_produto' para evitar conflito.
        colunas_produtos_interesse = ['codigo', 'nome', 'nomeGrupo', 'nomeCategoria']
        colunas_existentes = [col for col in colunas_produtos_interesse if col in df_produtos.columns]
        df_produtos_para_merge = df_produtos[colunas_existentes].copy()
        df_produtos_para_merge.rename(columns={'nome': 'nome_produto'}, inplace=True) # Renomeia a coluna
        df_produtos_para_merge['codigo'] = df_produtos_para_merge['codigo'].astype(str)
    else:
        df_produtos_para_merge = pd.DataFrame(columns=['codigo'])

    # Realiza as junções (MERGE) para enriquecer os dados.
    df_merged = pd.merge(df_vendas, df_vendedores[['codigo', 'nomeVendedor']], left_on='codigoVendedor', right_on='codigo', how='left')
    df_merged['nomeVendedor'] = df_merged['nomeVendedor'].fillna('Não encontrado')
    
    # Junta com as informações preparadas dos produtos.
    df_final = pd.merge(df_merged, df_produtos_para_merge, left_on='codigoProduto', right_on='codigo', how='left')
    
    # AJUSTADO: Garante que a coluna 'nome' exista e a preenche com 'nome_produto' se estiver vazia.
    if 'nome_produto' in df_final.columns:
        # NOVO: Verifica se a coluna 'nome' não foi criada (caso de um lote só com devoluções)
        if 'nome' not in df_final.columns:
            df_final['nome'] = None  # Cria a coluna vazia para evitar o erro

    df_final['nome'].fillna(df_final['nome_produto'], inplace=True)

    # Preenche valores nulos para as outras colunas para garantir que elas sempre existam.
    if 'nomeGrupo' not in df_final.columns: df_final['nomeGrupo'] = 'Não encontrado'
    if 'nomeCategoria' not in df_final.columns: df_final['nomeCategoria'] = 'Sem Categoria'
    df_final['nomeGrupo'].fillna('Não encontrado', inplace=True)
    df_final['nomeCategoria'].fillna('Sem Categoria', inplace=True)

    # Remove colunas de código redundantes e a coluna temporária 'nome_produto'.
    df_final.drop(columns=['codigo_x', 'codigo_y', 'codigo', 'nome_produto'], errors='ignore', inplace=True)
    # <<< FIM DA CORREÇÃO >>>

    # Garante que os valores de devolução sejam negativos para que as somas fiquem corretas.
    colunas_financeiras = ['valorTotalCusto', 'valorTotalBruto', 'valorTotalLiquido', 'quantidadeProdutos']
    for col in colunas_financeiras:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0)
            devolucoes = (df_final['status_venda'] == 'DEVOLUÇÃO')
            df_final.loc[devolucoes, col] = -df_final.loc[devolucoes, col].abs()

    # Salva a tabela processada, substituindo a versão anterior.
    _escrever_para_db(df_final, 'vendas_processadas', if_exists='replace')
    logging.info(f"Sucesso! Tabela 'vendas_processadas' foi atualizada com {len(df_final)} linhas.")

def sincronizar_estoque():
    """
    Busca as alterações de estoque do dia e atualiza a coluna 'quantidadeEstoque'
    na tabela 'produtos'.
    """
    NOME_TABELA = 'produtos'
    logging.info("\nIniciando sincronização de estoque...")

    hoje_str = datetime.now().strftime('%Y-%m-%d')
    params = {"dataInicial": hoje_str, "dataFinal": hoje_str}
    
    # Busca dados do endpoint de alteração de estoque.
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

    # Prepara os dados de estoque para a atualização.
    df_estoque.rename(columns={'codigoProduto': 'codigo'}, inplace=True)
    df_estoque_update = df_estoque[['codigo', 'quantidadeEstoque']].copy()
    
    df_produtos['codigo'] = df_produtos['codigo'].astype(str)
    df_estoque_update['codigo'] = df_estoque_update['codigo'].astype(str)
    
    # Usa o 'código' do produto como índice para uma atualização eficiente.
    df_produtos.set_index('codigo', inplace=True)
    df_estoque_update.set_index('codigo', inplace=True)
    
    # A função 'update' do pandas atualiza os valores em 'df_produtos' com base
    # nos valores correspondentes (pelo índice) em 'df_estoque_update'.
    df_produtos.update(df_estoque_update)
    
    df_produtos.reset_index(inplace=True) # Restaura o índice para o padrão.
    
    _escrever_para_db(df_produtos, NOME_TABELA, if_exists='replace')

def realizar_carga_historica_compras():
    """
    Executa a carga completa do histórico de compras desde a data definida em
    `HISTORICAL_START_DATE`. Utiliza o sistema de checkpoint para ser retomada em caso de falha.
    """
    NOME_TAREFA = 'carga_historica_compras'
    NOME_TABELA = 'compras'
    logging.info(f"\nIniciando carga completa das COMPRAS...")

    estado = _carregar_estado(NOME_TAREFA)
    ultima_data_concluida_str = estado.get('ultima_data_concluida')

    data_inicio_historico = pd.to_datetime(cfg.HISTORICAL_START_DATE)
    data_inicio_loop = data_inicio_historico
    if ultima_data_concluida_str:
        data_inicio_loop = datetime.strptime(ultima_data_concluida_str, '%Y-%m-%d') + timedelta(days=cfg.SALES_FILE_DAYS_INTERVAL)

    data_fim_loop = datetime.now()
    data_atual_periodo = data_inicio_loop
    
    df_existente = _ler_do_db(NOME_TABELA)

    while data_atual_periodo <= data_fim_loop:
        data_fim_periodo = data_atual_periodo + timedelta(days=cfg.SALES_FILE_DAYS_INTERVAL - 1)
        if data_fim_periodo > data_fim_loop: data_fim_periodo = data_fim_loop
        
        data_inicio_str = data_atual_periodo.strftime('%Y-%m-%d')
        data_fim_str = data_fim_periodo.strftime('%Y-%m-%d')
        logging.info(f"\nProcessando período de compras de {data_inicio_str} a {data_fim_str}")
        
        params_periodo = {"dataInicial": data_inicio_str, "dataFinal": data_fim_str}
        dados_compras = _buscar_dados_paginados(cfg.COMPRAS_ALT_ENDPOINT, params=params_periodo)
        
        if dados_compras is None:
            logging.error("Falha ao buscar compras para o período. A tarefa será retomada na próxima execução.")
            return

        if not dados_compras:
            logging.info("Nenhuma compra encontrada para o período.")
        else:
            df_compras_periodo = pd.DataFrame(dados_compras)
            try:
                # Junta os dados novos com os já existentes e remove duplicatas pela chave da nota
                df_final = pd.concat([df_existente, df_compras_periodo]).drop_duplicates(subset=['numeroNotaFiscal'], keep='last')
                _escrever_para_db(df_final, NOME_TABELA, if_exists='replace')
                df_existente = df_final.copy()
            except Exception as e:
                logging.error(f"Falha ao salvar o período de compras no banco de dados. Erro: {e}", exc_info=True)
                return
        
        _salvar_estado(NOME_TAREFA, {'ultima_data_concluida': data_atual_periodo.strftime('%Y-%m-%d')})
        data_atual_periodo += timedelta(days=cfg.SALES_FILE_DAYS_INTERVAL)
        
    logging.info(f"Tarefa '{NOME_TAREFA}' concluída com sucesso.")
    _limpar_estado(NOME_TAREFA)


def atualizar_compras_recentes():
    """
    Busca as notas de compra do dia corrente que foram criadas ou alteradas.
    Ideal para atualizações frequentes.
    """
    NOME_TABELA = 'compras'
    logging.info("\nIniciando atualização de compras recentes...")
    
    df_existente = _ler_do_db(NOME_TABELA)
    logging.info(f"Encontrados {len(df_existente)} registros existentes na tabela '{NOME_TABELA}'.")
    
    hoje_str = datetime.now().strftime('%Y-%m-%d')
    params_alt = {"dataInicial": hoje_str, "dataFinal": hoje_str}
    dados_alterados = _buscar_dados_paginados(cfg.COMPRAS_ALT_ENDPOINT, params=params_alt)

    if not dados_alterados:
        logging.info("Nenhuma compra nova ou alterada para processar.")
        return
        
    df_alterados = pd.DataFrame(dados_alterados)
    logging.info(f"{len(df_alterados)} compras novas/alteradas encontradas.")

    # Lógica de atualização: remove as versões antigas das notas que foram alteradas
    # e depois adiciona as novas versões.
    ids_para_remover = df_alterados['numeroNotaFiscal'].unique()
    df_intermediario = df_existente[~df_existente['numeroNotaFiscal'].isin(ids_para_remover)]
    
    # Junta o DataFrame original (sem as notas alteradas) com as novas versões.
    df_final = pd.concat([df_intermediario, df_alterados]).drop_duplicates(subset=['numeroNotaFiscal'], keep='last')
    _escrever_para_db(df_final, NOME_TABELA, if_exists='replace')

def sincronizar_fornecedores_carga_inicial():
    """
    Executa a carga inicial de fornecedores.
    Busca e salva a lista COMPLETA de fornecedores recebida da API.
    Esta função é projetada para ser executada apenas uma vez.
    """
    NOME_TABELA = 'fornecedores'
    logging.info("\nIniciando carga inicial de FORNECEDORES...")

    dados_fornecedores = _buscar_dados_paginados(cfg.FORNECEDOR_ENDPOINT)

    if dados_fornecedores is None:
        logging.error("Falha crítica ao buscar dados para a carga inicial de fornecedores. A tarefa foi abortada.")
        return

    if not dados_fornecedores:
        logging.info("Nenhum fornecedor encontrado na API.")
        _escrever_para_db(pd.DataFrame(), NOME_TABELA, if_exists='replace')
        return

    df_fornecedores = pd.DataFrame(dados_fornecedores)

    # Lógica ajustada: Salva todos os fornecedores, pois não há campo de data para filtro.
    logging.info(f"Recebidos {len(df_fornecedores)} registros. Salvando todos os fornecedores, conforme esperado para este endpoint.")
    _escrever_para_db(df_fornecedores, NOME_TABELA, if_exists='replace')

    logging.info("Carga inicial de fornecedores concluída com sucesso.")

def atualizar_fornecedores_recentes():
    """
    Busca fornecedores que foram criados ou alterados no dia corrente e
    atualiza a tabela local.
    """
    NOME_TABELA = 'fornecedores'
    logging.info("\nIniciando atualização de fornecedores recentes...")

    hoje_str = datetime.now().strftime('%Y-%m-%d')
    params = {"dataInicial": hoje_str, "dataFinal": hoje_str}
    
    dados_alterados = _buscar_dados_paginados(cfg.FORNECEDOR_ALT_ENDPOINT, params=params)

    if not dados_alterados:
        logging.info("Nenhum fornecedor novo ou alterado para processar.")
        return

    df_alterados = pd.DataFrame(dados_alterados)
    logging.info(f"{len(df_alterados)} fornecedores novos/alterados encontrados.")

    df_existente = _ler_do_db(NOME_TABELA)
    
    # Lógica de atualização: junta os fornecedores existentes com os alterados,
    # mantendo a versão mais recente em caso de duplicatas (baseado no código).
    # Assumimos que 'codigo' é o identificador único do fornecedor.
    df_final = pd.concat([df_existente, df_alterados]).drop_duplicates(subset=['codigo'], keep='last')
    
    _escrever_para_db(df_final, NOME_TABELA, if_exists='replace')