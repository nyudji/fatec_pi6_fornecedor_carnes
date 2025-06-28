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

        # Cria as tabelas do banco de dados (chamando os métodos que usam self.execute_query)
        # Como o PostgresConnect agora está autocommit=True (para DDL), cada create_table fará seu próprio commit.
        #self.create_table_tb_fornecedor()
        #self.create_table_tb_produto()
        #self.create_table_tb_entrada()
        #self.create_table_tb_produto_entrada()
        #self.create_table_tb_estoque()
        #self.create_table_tb_cliente()
        #self.create_table_tb_pedido()
        ##self.create_table_tb_item_pedido()
        #self.create_table_tb_pagamento()
        #print("Todas as verificações de tabela foram executadas.")


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


    # # --- Criação das tabelas do banco de dados ---
    # # Estes métodos agora chamam self.execute_query() que, por sua vez,
    # # usa o cursor da conexão persistente e faz commit se autocommit for True.
    # # Não é mais necessário criar/fechar cursor ou fazer commit/rollback aqui.

    # def create_table_tb_fornecedor(self):
    #     query = '''
    #         CREATE TABLE IF NOT EXISTS tb_fornecedor (
    #             id_fornecedor SERIAL PRIMARY KEY,
    #             nome_fornecedor VARCHAR(255),
    #             cnpj_fornecedor VARCHAR(18) UNIQUE,
    #             telefone_fornecedor VARCHAR(20),
    #             email_fornecedor VARCHAR(100),
    #             endereco_fornecedor TEXT
    #         );
    #     '''
    #     self.execute_query(query) 
    #     # print("Tabela tb_fornecedor verificada/criada.") # Opcional: para debug

    # def create_table_tb_produto(self):
    #     query = '''
    #         CREATE TABLE IF NOT EXISTS tb_produto (
    #             id_produto SERIAL PRIMARY KEY,
    #             nome_produto VARCHAR(255),
    #             tipo_corte VARCHAR(100),
    #             unidade_medida VARCHAR(10),
    #             preco_compra NUMERIC(10,2),
    #             preco_venda NUMERIC(10,2),
    #             id_fornecedor INTEGER REFERENCES tb_fornecedor(id_fornecedor) ON DELETE SET NULL
    #     '''
    #     self.execute_query(query)
    #     # print("Tabela tb_produto verificada/criada.")

    # def create_table_tb_entrada(self):
    #     query = '''
    #         CREATE TABLE IF NOT EXISTS tb_entrada (
    #             id_entrada SERIAL PRIMARY KEY,
    #             data_entrada DATE,
    #             id_fornecedor INTEGER REFERENCES tb_fornecedor(id_fornecedor) ON DELETE SET NULL
    #         );
    #     '''
    #     self.execute_query(query)
    #     # print("Tabela tb_entrada verificada/criada.")

    # def create_table_tb_produto_entrada(self):
    #     query = '''
    #         CREATE TABLE IF NOT EXISTS tb_produto_entrada (
    #             id_item_entrada SERIAL PRIMARY KEY,
    #             id_entrada INTEGER REFERENCES tb_entrada(id_entrada) ON DELETE CASCADE,
    #             id_produto INTEGER REFERENCES tb_produto(id_produto) ON DELETE CASCADE,
    #             quantidade INTEGER,
    #             preco_total NUMERIC(10,2),
    #             validade DATE,
    #             lote VARCHAR(50)
    #         );
    #     '''
    #     self.execute_query(query)
    #     # print("Tabela tb_produto_entrada verificada/criada.")

    # def create_table_tb_estoque(self):
    #     query = '''
    #         CREATE TABLE IF NOT EXISTS tb_estoque (
    #             id_estoque SERIAL PRIMARY KEY,
    #             item_entrada INTEGER REFERENCES tb_produto_entrada(id_item_entrada) ON DELETE CASCADE,
    #             quantidade_disponivel INTEGER,
    #             localizacao VARCHAR(100)
    #         );
    #     '''
    #     self.execute_query(query)
    #     # print("Tabela tb_estoque verificada/criada.")

    # def create_table_tb_cliente(self):
    #     query = '''
    #         CREATE TABLE IF NOT EXISTS tb_cliente (
    #             id_cliente SERIAL PRIMARY KEY,
    #             nome_cliente VARCHAR(100),
    #             cnpj_cliente VARCHAR(20),
    #             endereco_cliente TEXT,
    #             telefone_cliente VARCHAR(20),
    #             email_cliente VARCHAR(100),
    #             tipo_cliente VARCHAR(20)
    #         );
    #     '''
    #     self.execute_query(query)
    #     # print("Tabela tb_cliente verificada/criada.")

    # def create_table_tb_pedido(self):
    #     query = '''
    #         CREATE TABLE IF NOT EXISTS tb_pedido (
    #             id_pedido SERIAL PRIMARY KEY,
    #             id_cliente INTEGER REFERENCES tb_cliente(id_cliente) ON DELETE SET NULL,
    #             data_pedido DATE,
    #             status VARCHAR(30),
    #             valor_total NUMERIC(10,2)
    #         );
    #     '''
    #     self.execute_query(query)
    #     # print("Tabela tb_pedido verificada/criada.")

    # def create_table_tb_item_pedido(self):
    #     query = '''
    #         CREATE TABLE IF NOT EXISTS tb_item_pedido (
    #             id_item_pedido SERIAL PRIMARY KEY,
    #             id_pedido INTEGER REFERENCES tb_pedido(id_pedido) ON DELETE CASCADE,
    #             id_produto INTEGER REFERENCES tb_produto(id_produto) ON DELETE SET NULL,
    #             quantidade INTEGER,
    #             unidade_medida VARCHAR(10),
    #             preco_unitario NUMERIC(10,2)
    #         );
    #     '''
    #     self.execute_query(query)
    #     # print("Tabela tb_item_pedido verificada/criada.")
    
    # def create_table_tb_pagamento(self):
    #     query = '''
    #         CREATE TABLE IF NOT EXISTS tb_pagamento (
    #             id_pagamento SERIAL PRIMARY KEY,
    #             id_pedido INTEGER REFERENCES tb_pedido(id_pedido) ON DELETE CASCADE,
    #             data_pagamento DATE,
    #             lote_saida VARCHAR(50),
    #             valor_pago NUMERIC(10,2),
    #             metodo_pagamento VARCHAR(50),
    #             status VARCHAR(30)
    #         );
    #     '''
    #     self.execute_query(query)
    #     # print("Tabela tb_pagamento verificada/criada.")
    