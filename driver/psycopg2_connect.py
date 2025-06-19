# driver/psycopg2_connect.py
# Importando Libs
from dotenv import load_dotenv, find_dotenv
import os
import psycopg2
# import urllib.parse # REMOVIDO: Não é necessário para psycopg2

# Carregar variáveis do arquivo .env
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

class PostgresConnect:
    '''Classe responsável por gerenciar a conexão com o banco de dados usando psycopg2'''

    def __init__(self, autocommit=False): # Adicionando parâmetro autocommit
        self.username = os.getenv("USER_DB")
        self.password = os.getenv("PASSWORD_DB")
        self.host = os.getenv("HOST_DB")
        self.port = os.getenv("PORT_DB")
        self.database = os.getenv("NAME_DB")
        
        self.conn = None # A conexão é inicializada como None
        self.autocommit = autocommit # Armazena o estado de autocommit
        self._cursor = None # Inicializa o cursor como None
        self._connect() # Tenta estabelecer a conexão ao inicializar a classe

    def _connect(self): # Renomeado para _connect para indicar que é um método interno
        '''Cria a conexão com o banco de dados PostgreSQL e define o autocommit.'''
        try:
            # === CORREÇÃO: Removendo o urllib.parse.quote_plus() ===
            # psycog2 lida com a senha diretamente, sem a necessidade de codificação.
            # Certifique-se de que self.port é um inteiro
            port_int = int(self.port) if self.port else 5432 

            self.conn = psycopg2.connect(
                dbname=self.database,
                user=self.username,
                password=self.password,
                host=self.host,
                port=port_int
            )
            self.conn.autocommit = self.autocommit # Aplica o autocommit configurado
            print("Conexão com o PostgreSQL estabelecida com sucesso!")
        except Exception as e:
            print(f"Erro ao conectar ao PostgreSQL: {e}")
            self.conn = None # Garante que a conexão seja None se falhar
        
    def get_cursor(self): # === NOVO MÉTODO: Para obter o cursor ===
        '''Retorna um cursor para a conexão ativa. Cria um novo se ainda não existir ou estiver fechado.'''
        if self.conn and not self.conn.closed:
            if self._cursor is None or self._cursor.closed:
                self._cursor = self.conn.cursor()
            return self._cursor
        else:
            print("Erro: Conexão não está ativa ou foi fechada. Impossível obter cursor.")
            return None

    def execute_query(self, query, params=None): # === CORREÇÃO: REMOVIDO self.conn.close() ===
        """Executa uma consulta SQL sem retorno de dados. Usa o commit se não for autocommit."""
        if self.conn and not self.conn.closed:
            try:
                cur = self.get_cursor() # Obtém o cursor através do novo método
                if cur:
                    cur.execute(query, params)
                    if not self.autocommit: # Apenas faz commit se não estiver em modo autocommit
                        self.conn.commit()
            except Exception as e:
                print(f"Erro ao executar query: {e}")
                if self.conn and not self.autocommit:
                    self.conn.rollback() # Tenta rollback em caso de erro
        else:
            print("Erro: Conexão não está ativa para executar query.")

    def commit(self): # === NOVO MÉTODO: Para commit manual ===
        '''Realiza o commit das transações pendentes se o autocommit for False.'''
        if self.conn and not self.conn.closed and not self.autocommit:
            try:
                self.conn.commit()
                # print("Transação commited.") # Opcional: para debug
            except Exception as e:
                print(f"Erro ao realizar commit: {e}")
                self.rollback() # Tenta rollback em caso de erro no commit

    def rollback(self): # === NOVO MÉTODO: Para rollback manual ===
        '''Realiza o rollback das transações pendentes se o autocommit for False.'''
        if self.conn and not self.conn.closed and not self.autocommit:
            try:
                self.conn.rollback()
                # print("Transação rollbacked.") # Opcional: para debug
            except Exception as e:
                print(f"Erro ao realizar rollback: {e}")

    def close_connection(self):
        '''Fecha o cursor e a conexão com o banco de dados.'''
        try:
            if self._cursor and not self._cursor.closed:
                self._cursor.close()
                self._cursor = None
            if self.conn and not self.conn.closed:
                self.conn.close()
                print("Conexão com o PostgreSQL fechada.")
        except Exception as e:
            print(f"Erro ao fechar a conexão com o banco de dados: {e}")