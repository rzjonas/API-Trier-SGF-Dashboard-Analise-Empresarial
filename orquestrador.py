import time
import os
from datetime import datetime, timedelta
import logging
import sqlite3

import config_conexao as cfg

import conexao_api_trier_sgf as api

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
file_handler = logging.FileHandler('log.txt', mode='a')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def _tabelas_iniciais_existem():
    if not os.path.exists(cfg.DATABASE_FILE):
        return False
        
    try:
        conn = sqlite3.connect(cfg.DATABASE_FILE)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('vendas', 'produtos', 'vendedores')")
        tabelas = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        return all(t in tabelas for t in ['vendas', 'produtos', 'vendedores'])
        
    except Exception as e:
        logging.error(f"Erro ao verificar tabelas no banco de dados: {e}")
        return False

def main():
    """
    Função principal que orquestra a execução de todas as tarefas de sincronização.
    """
    logging.info("=============================================")
    logging.info("=      INICIANDO ORQUESTRADOR DE DADOS      =")
    logging.info(f"=      Data e Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}      =")
    logging.info("=============================================")
    logging.info(f"Usando banco de dados em: {cfg.DATABASE_FILE}")

    logging.info("\n[FASE DE INICIALIZAÇÃO]")
    logging.info("Verificando se a carga de dados inicial é necessária...")
    
    if not _tabelas_iniciais_existem():
        logging.info("Banco de dados ou tabelas essenciais não encontrados. Executando rotinas de carga inicial...")
        try:
            api.sincronizar_produtos(carga_inicial=True)
        except Exception as e:
            logging.error(f"Erro inesperado durante a sincronização de produtos: {e}", exc_info=True)
        
        try:
            api.sincronizar_vendedores()
        except Exception as e:
            logging.error(f"Erro inesperado durante a sincronização de vendedores: {e}", exc_info=True)
            
        try:
            api.realizar_carga_historica_vendas()
            api.processar_e_salvar_dados_analiticos()
        except Exception as e:
            logging.error(f"Erro inesperado durante a carga histórica de vendas: {e}", exc_info=True)

        logging.info("Carga inicial e processamento de todos os dados foram concluídos.")
    else:
        logging.info("Banco de dados e tabelas encontrados. Pulando para o ciclo de atualização contínua.")

    logging.info("\n[FASE DE ORQUESTRAÇÃO]")
    logging.info("Entrando no ciclo de atualização contínua. Pressione CTRL+C para sair.")
    
    agora = datetime.now()
    proxima_exec_vendas = agora
    proxima_exec_produtos = agora
    proxima_exec_estoque = agora
    proxima_exec_vendedores = agora
    
    try:
        while True:
            agora = datetime.now()
            
            if agora >= proxima_exec_vendas:
                logging.info(f"\n--- {agora.strftime('%Y-%m-%d %H:%M:%S')} ---")
                logging.info("EXECUTANDO: Atualização de VENDAS")
                try:
                    api.atualizar_vendas_recentes()
                    api.processar_e_salvar_dados_analiticos()
                except Exception as e:
                    logging.error(f"Erro inesperado no ciclo de vendas: {e}", exc_info=True)
                proxima_exec_vendas = agora + timedelta(minutes=cfg.INTERVALO_VENDAS)
                logging.info(f"AGENDADO: Próxima execução de vendas para {proxima_exec_vendas.strftime('%H:%M:%S')}")

            if agora >= proxima_exec_produtos:
                logging.info(f"\n--- {agora.strftime('%Y-%m-%d %H:%M:%S')} ---")
                logging.info("EXECUTANDO: 'sincronizar_produtos'")
                try:
                    api.sincronizar_produtos(carga_inicial=False)
                except Exception as e:
                    logging.error(f"Erro inesperado em 'sincronizar_produtos': {e}", exc_info=True)
                proxima_exec_produtos = agora + timedelta(minutes=cfg.INTERVALO_PRODUTOS)
                logging.info(f"AGENDADO: Próxima execução de produtos para {proxima_exec_produtos.strftime('%H:%M:%S')}")

            if agora >= proxima_exec_estoque:
                logging.info(f"\n--- {agora.strftime('%Y-%m-%d %H:%M:%S')} ---")
                logging.info("EXECUTANDO: 'sincronizar_estoque'")
                try:
                    api.sincronizar_estoque()
                except Exception as e:
                    logging.error(f"Erro inesperado em 'sincronizar_estoque': {e}", exc_info=True)
                proxima_exec_estoque = agora + timedelta(minutes=cfg.INTERVALO_ESTOQUE)
                logging.info(f"AGENDADO: Próxima execução de estoque para {proxima_exec_estoque.strftime('%H:%M:%S')}")

            if agora >= proxima_exec_vendedores:
                logging.info(f"\n--- {agora.strftime('%Y-%m-%d %H:%M:%S')} ---")
                logging.info("EXECUTANDO: 'sincronizar_vendedores'")
                try:
                    api.sincronizar_vendedores()
                except Exception as e:
                    logging.error(f"Erro inesperado em 'sincronizar_vendedores': {e}", exc_info=True)
                proxima_exec_vendedores = agora + timedelta(minutes=cfg.INTERVALO_VENDEDORES)
                logging.info(f"AGENDADO: Próxima execução de vendedores para {proxima_exec_vendedores.strftime('%H:%M:%S')}")
            
            time.sleep(60)

    except KeyboardInterrupt:
        logging.info("\n\n=============================================")
        logging.info("=     ORQUESTRADOR INTERROMPIDO PELO USUÁRIO      =")
        logging.info("=============================================")

if __name__ == "__main__":
    main()