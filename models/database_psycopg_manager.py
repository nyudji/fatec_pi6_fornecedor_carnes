# manager.py
# Import Modulos
# Importa PostgresConnect e a renomeia para driver para usar como classe pai
from driver.psycopg2_connect import PostgresConnect 
from psycopg2.extras import execute_values

# Import Libs
import pandas as pd
import streamlit as st

class Manage_database(PostgresConnect): # Não precisa de 'as driver' aqui, já que é uma classe pai
    '''
    Classe responsável por gerenciar as operações no banco de dados, 
    se baseia na PostgresConnect (classe pai, driver do sgbd)
    para fazer a conexão com o banco. 
    '''

    def __init__(self, autocommit=False):
        # Chama o __init__ da classe pai (PostgresConnect)
        # Passa autocommit=True para DDLs (CREATE TABLE) para que cada criação seja salva automaticamente.
        super().__init__(autocommit=autocommit) 
        
        # === VERIFICA SE A CONEXÃO FOI BEM SUCEDIDA ANTES DE TENTAR CRIAR TABELAS ===
        if self.conn is None:
            print("Não foi possível conectar ao banco de dados. As tabelas não serão criadas.")
            return # Sai do __init__ se a conexão falhou

    def read_table(self, table_name, columns=None, where=None):
        '''
        Lê uma tabela do banco de dados e retorna um DataFrame.
        '''
        # O "america_gestao" mencionado no seu docstring não se aplica aqui, 
        # já que o dbname vem do .env agora.
        
        if self.conn is None or self.conn.closed:
            print("Erro: Conexão com o banco de dados não está ativa para leitura.")
            return None

        try:
            query = (
                f"SELECT {', '.join(columns) if columns else '*'} "
                f"FROM {table_name}"
            )
            if where:
                query += f" WHERE {where}"

            # pd.read_sql pode usar a própria conexão do psycopg2 diretamente
            df = pd.read_sql(query, self.conn) 
            print(f"Tabela '{table_name}' lida com sucesso.")
            return df
        except Exception as e:
            print(f"Erro ao ler a tabela {table_name}: {e}")
            return None
        
    def insert_dataframe_batch(self, table_name, df_to_insert, id_column_to_exclude=None):
        if self.conn is None or self._cursor is None: # Usando self._cursor
            st.error("Conexão ou cursor do banco de dados não está ativa para inserção em lote.")
            return False
        
        if df_to_insert.empty:
            st.info(f" Nenhum dado no CSV para importar para '{table_name}'.")
            return True

        insert_df = df_to_insert.copy()
        if id_column_to_exclude and id_column_to_exclude in insert_df.columns:
            st.info(f"Removendo a coluna '{id_column_to_exclude}' do CSV para inserção em '{table_name}'.")
            insert_df = insert_df.drop(columns=[id_column_to_exclude])

        columns = insert_df.columns.tolist()
        
        # --- ALTERAÇÃO AQUI: REMOVENDO AS ASPAS DUPLAS DOS NOMES DAS COLUNAS ---
        # Agora, a string de colunas será 'col1, col2, col3' em vez de '"col1", "col2", "col3"'
        cols_str = ', '.join([c for c in columns]) 
        
        values = [tuple(row) for row in insert_df.itertuples(index=False)]

        try:
            # --- ALTERAÇÃO AQUI: REMOVENDO AS ASPAS DUPLAS DO NOME DA TABELA ---
            # A query será INSERT INTO table_name (...) VALUES %s
            query = f'INSERT INTO {table_name} ({cols_str}) VALUES %s' 
            
            execute_values(self._cursor, query, values) # Usando self._cursor
            self.commit()
            st.success(f"✅ {len(values)} registros importados com sucesso para '{table_name}'!")
            return True
        except Exception as e:
            self.rollback()
            st.error(f"Erro ao inserir dados em lote na tabela '{table_name}': {e}\n"
                     f"Verifique se as colunas do CSV correspondem às colunas da tabela: {columns}")
            return False
    