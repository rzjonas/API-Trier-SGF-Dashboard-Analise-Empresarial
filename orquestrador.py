# ==============================================================================
# IMPORTAÇÃO DE BIBLIOTECAS
# ==============================================================================
# Módulos padrão do Python para funcionalidades essenciais.
import time  # Usado para pausar a execução do loop principal (time.sleep).
import os    # Para interagir com o sistema operacional, como verificar a existência de arquivos (os.path.exists).
from datetime import datetime, timedelta  # Para manipular datas e horas, usado para agendar as próximas execuções.
import logging  # Para registrar informações, avisos e erros em um arquivo de log e no console.
import sqlite3  # Para interagir diretamente com o banco de dados SQLite na verificação inicial.

# ==============================================================================
# IMPORTAÇÃO DE MÓDULOS DO PROJETO
# ==============================================================================
# Importa as configurações globais, como caminhos de arquivos e intervalos de tempo.
import config_conexao as cfg

# Importa o módulo que contém todas as funções de interação com a API e o banco de dados.
import conexao_api_trier_sgf as api

# ==============================================================================
# CONFIGURAÇÃO DO LOGGING
# ==============================================================================
# Define um formato padrão para as mensagens de log, incluindo data, hora, nível e a mensagem.
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Obtém a instância principal do logger.
logger = logging.getLogger()
# Define o nível mínimo de log a ser capturado (INFO e acima: INFO, WARNING, ERROR, CRITICAL).
logger.setLevel(logging.INFO)

# Limpa handlers existentes para evitar duplicação de logs caso o script seja recarregado.
if logger.hasHandlers():
    logger.handlers.clear()

# Cria um handler para escrever os logs em um arquivo chamado 'log.txt' no modo 'append' (adicionar ao final).
file_handler = logging.FileHandler('log.txt', mode='a')
file_handler.setFormatter(formatter)  # Aplica o formato definido ao handler de arquivo.

# Cria um handler para exibir os logs no console (saída padrão).
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter) # Aplica o mesmo formato ao handler de console.

# Adiciona os dois handlers (arquivo e console) ao logger principal.
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================
def _tabelas_iniciais_existem():
    """
    Verifica se o arquivo de banco de dados e as tabelas essenciais
    ('vendas', 'produtos', 'vendedores', 'compras', 'fornecedores') já existem.
    Isso é crucial para determinar se a carga inicial de dados deve ser executada.

    Retorna:
        bool: True se o banco e as tabelas existirem, False caso contrário.
    """
    # Se o arquivo do banco de dados não existir, não há necessidade de continuar a verificação.
    if not os.path.exists(cfg.DATABASE_FILE):
        return False
        
    try:
        # Conecta-se ao banco de dados SQLite.
        conn = sqlite3.connect(cfg.DATABASE_FILE)
        cursor = conn.cursor()
        
        # Lista de tabelas essenciais para a aplicação.
        tabelas_essenciais = ['vendas', 'produtos', 'vendedores', 'compras', 'fornecedores']
        
        # Constrói a consulta para verificar a existência de todas as tabelas de uma vez.
        placeholders = ', '.join('?' for _ in tabelas_essenciais)
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({placeholders})"
        cursor.execute(query, tabelas_essenciais)
        
        # Extrai os nomes das tabelas encontradas do resultado da consulta.
        tabelas_encontradas = {row[0] for row in cursor.fetchall()}
        
        # Fecha a conexão com o banco de dados.
        conn.close()
        
        # Retorna True somente se o conjunto de tabelas encontradas for igual ao conjunto de tabelas essenciais.
        return set(tabelas_essenciais) == tabelas_encontradas
        
    except Exception as e:
        # Em caso de qualquer erro durante a verificação, registra o problema e retorna False.
        logging.error(f"Erro ao verificar tabelas no banco de dados: {e}")
        return False

# ==============================================================================
# FUNÇÃO PRINCIPAL (ORQUESTRADOR)
# ==============================================================================
def main():
    """
    Função principal que orquestra a execução de todas as tarefas de sincronização.
    - Primeiro, verifica se uma carga inicial é necessária.
    - Depois, entra em um loop infinito para atualizações contínuas e agendadas.
    """
    # Log inicial para marcar o início de uma nova execução do orquestrador.
    logging.info("=============================================")
    logging.info("=      INICIANDO ORQUESTRADOR DE DADOS      =")
    logging.info(f"=      Data e Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}      =")
    logging.info("=============================================")
    logging.info(f"Usando banco de dados em: {cfg.DATABASE_FILE}")

    # --- FASE DE INICIALIZAÇÃO ---
    logging.info("\n[FASE DE INICIALIZAÇÃO]")
    logging.info("Verificando se a carga de dados inicial é necessária...")
    
    # Chama a função auxiliar para verificar a existência do banco e tabelas.
    if not _tabelas_iniciais_existem():
        logging.info("Banco de dados ou tabelas essenciais não encontrados. Executando rotinas de carga inicial...")
        
        # Bloco de carga inicial: executa as funções de sincronização pela primeira vez.
        # Cada chamada é envolvida por um try-except para evitar que um erro em uma tarefa impeça as outras de rodar.
        try:
            # Sincroniza todos os produtos da API para a tabela 'produtos'.
            api.sincronizar_produtos(carga_inicial=True)
        except Exception as e:
            logging.error(f"Erro inesperado durante a sincronização de produtos: {e}", exc_info=True)
        
        try:
            # Sincroniza todos os vendedores da API para a tabela 'vendedores'.
            api.sincronizar_vendedores()
        except Exception as e:
            logging.error(f"Erro inesperado durante a sincronização de vendedores: {e}", exc_info=True)

        try:
            # (NOVO) Realiza a carga inicial de fornecedores.
            api.sincronizar_fornecedores_carga_inicial()
        except Exception as e:
            logging.error(f"Erro inesperado durante a carga inicial de fornecedores: {e}", exc_info=True)
            
        try:
            # Realiza a carga completa de todo o histórico de vendas.
            api.realizar_carga_historica_vendas()
            # Após a carga, processa os dados brutos e salva na tabela 'vendas_processadas'.
            api.processar_e_salvar_dados_analiticos()
        except Exception as e:
            logging.error(f"Erro inesperado durante a carga histórica de vendas: {e}", exc_info=True)

        try:
            # Realiza a carga completa de todo o histórico de compras.
            api.realizar_carga_historica_compras()
        except Exception as e:
            logging.error(f"Erro inesperado durante a carga histórica de compras: {e}", exc_info=True)

        logging.info("Carga inicial e processamento de todos os dados foram concluídos.")

    else:
        # Se as tabelas já existem, informa que a carga inicial será pulada.
        logging.info("Banco de dados e tabelas encontrados. Pulando para o ciclo de atualização contínua.")

    # --- FASE DE ORQUESTRAÇÃO CONTÍNUA ---
    logging.info("\n[FASE DE ORQUESTRAÇÃO]")
    logging.info("Entrando no ciclo de atualização contínua. Pressione CTRL+C para sair.")
    
    # Inicializa as variáveis de agendamento com a hora atual para garantir
    # que todas as tarefas sejam executadas na primeira iteração do loop.
    agora = datetime.now()
    proxima_exec_vendas = agora
    proxima_exec_compras = agora
    proxima_exec_produtos = agora
    proxima_exec_estoque = agora
    proxima_exec_vendedores = agora
    proxima_exec_fornecedores = agora # (NOVO)
    
    try:
        # Loop infinito que mantém o orquestrador rodando.
        while True:
            agora = datetime.now()
            
            # --- VERIFICAÇÃO E EXECUÇÃO DE TAREFAS AGENDADAS ---

            # 1. Tarefa de Vendas
            if agora >= proxima_exec_vendas:
                logging.info(f"\n--- {agora.strftime('%Y-%m-%d %H:%M:%S')} ---")
                logging.info("EXECUTANDO: Atualização de VENDAS")
                try:
                    # Busca apenas as vendas recentes (alteradas/novas/canceladas) e atualiza o banco.
                    api.atualizar_vendas_recentes()
                    # Reprocessa todos os dados para manter a tabela analítica atualizada.
                    api.processar_e_salvar_dados_analiticos()
                except Exception as e:
                    logging.error(f"Erro inesperado no ciclo de vendas: {e}", exc_info=True)
                # Reagenda a próxima execução desta tarefa.
                proxima_exec_vendas = agora + timedelta(minutes=cfg.INTERVALO_VENDAS)
                logging.info(f"AGENDADO: Próxima execução de vendas para {proxima_exec_vendas.strftime('%H:%M:%S')}")

            # 2. Tarefa de Produtos
            if agora >= proxima_exec_produtos:
                logging.info(f"\n--- {agora.strftime('%Y-%m-%d %H:%M:%S')} ---")
                logging.info("EXECUTANDO: 'sincronizar_produtos'")
                try:
                    # Sincroniza apenas os produtos alterados no dia.
                    api.sincronizar_produtos(carga_inicial=False)
                except Exception as e:
                    logging.error(f"Erro inesperado em 'sincronizar_produtos': {e}", exc_info=True)
                # Reagenda a próxima execução.
                proxima_exec_produtos = agora + timedelta(minutes=cfg.INTERVALO_PRODUTOS)
                logging.info(f"AGENDADO: Próxima execução de produtos para {proxima_exec_produtos.strftime('%H:%M:%S')}")

            # 3. Tarefa de Estoque
            if agora >= proxima_exec_estoque:
                logging.info(f"\n--- {agora.strftime('%Y-%m-%d %H:%M:%S')} ---")
                logging.info("EXECUTANDO: 'sincronizar_estoque'")
                try:
                    # Busca e atualiza o estoque dos produtos que tiveram movimentação.
                    api.sincronizar_estoque()
                except Exception as e:
                    logging.error(f"Erro inesperado em 'sincronizar_estoque': {e}", exc_info=True)
                # Reagenda a próxima execução.
                proxima_exec_estoque = agora + timedelta(minutes=cfg.INTERVALO_ESTOQUE)
                logging.info(f"AGENDADO: Próxima execução de estoque para {proxima_exec_estoque.strftime('%H:%M:%S')}")

            # 4. Tarefa de Vendedores (executa com menos frequência)
            if agora >= proxima_exec_vendedores:
                logging.info(f"\n--- {agora.strftime('%Y-%m-%d %H:%M:%S')} ---")
                logging.info("EXECUTANDO: 'sincronizar_vendedores'")
                try:
                    # Sincroniza a lista completa de vendedores.
                    api.sincronizar_vendedores()
                except Exception as e:
                    logging.error(f"Erro inesperado em 'sincronizar_vendedores': {e}", exc_info=True)
                # Reagenda a próxima execução.
                proxima_exec_vendedores = agora + timedelta(minutes=cfg.INTERVALO_VENDEDORES)
                logging.info(f"AGENDADO: Próxima execução de vendedores para {proxima_exec_vendedores.strftime('%H:%M:%S')}")

            # 5. Tarefa de Compras
            if agora >= proxima_exec_compras:
                logging.info(f"\n--- {agora.strftime('%Y-%m-%d %H:%M:%S')} ---")
                logging.info("EXECUTANDO: Atualização de COMPRAS")
                try:
                    # Busca apenas as compras recentes (alteradas/novas) e atualiza o banco.
                    api.atualizar_compras_recentes()
                except Exception as e:
                    logging.error(f"Erro inesperado no ciclo de compras: {e}", exc_info=True)
                # Reagenda a próxima execução desta tarefa.
                proxima_exec_compras = agora + timedelta(minutes=cfg.INTERVALO_COMPRAS)
                logging.info(f"AGENDADO: Próxima execução de compras para {proxima_exec_compras.strftime('%H:%M:%S')}")
            
            # 6. Tarefa de Fornecedores (NOVO)
            if agora >= proxima_exec_fornecedores:
                logging.info(f"\n--- {agora.strftime('%Y-%m-%d %H:%M:%S')} ---")
                logging.info("EXECUTANDO: Atualização de FORNECEDORES")
                try:
                    # Busca apenas os fornecedores recentes (alterados/novos) e atualiza o banco.
                    api.atualizar_fornecedores_recentes()
                except Exception as e:
                    logging.error(f"Erro inesperado no ciclo de fornecedores: {e}", exc_info=True)
                # Reagenda a próxima execução desta tarefa.
                proxima_exec_fornecedores = agora + timedelta(minutes=cfg.INTERVALO_FORNECEDORES)
                logging.info(f"AGENDADO: Próxima execução de fornecedores para {proxima_exec_fornecedores.strftime('%H:%M:%S')}")

            # Pausa a execução por 60 segundos antes de verificar novamente os agendamentos.
            # Isso evita que o loop consuma 100% do processador.
            time.sleep(60)

    except KeyboardInterrupt:
        # Captura o comando de interrupção (CTRL+C) para encerrar o script de forma limpa.
        logging.info("\n\n=============================================")
        logging.info("=     ORQUESTRADOR INTERROMPIDO PELO USUÁRIO      =")
        logging.info("=============================================")

# ==============================================================================
# PONTO DE ENTRADA DO SCRIPT
# ==============================================================================
# Garante que a função 'main' seja executada apenas quando o script é chamado diretamente.
if __name__ == "__main__":
    main()