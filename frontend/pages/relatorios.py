import streamlit as st
import pandas as pd
import time
from models.database_psycopg_manager import Manage_database

def crud_section(title, table_name, columns, id_column, db_manager):
    st.header(title)
    df = db_manager.read_table(table_name)
    
    # Exibe a tabela antes do formulário de inserção
    st.subheader(f"Tabela de {title}")
    if df is not None and not df.empty:
        st.dataframe(df)
    else:
        st.info("Nenhum registro encontrado.")
    st.subheader(f"Exportar/Importar para CSV")
    col_export, col_import = st.columns(2)
    with col_export:
        st.subheader(f"Exportar")
        if df is not None and not df.empty:
            # Converte o DataFrame para CSV e o codifica para UTF-8
            csv_data = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label=f"Baixar {title}.csv",
                data=csv_data,
                file_name=f"{table_name}.csv",
                mime="text/csv",
                key=f"download_{table_name}"
            )
        else:
            st.info("Não há dados para exportar.")
    with col_import:
        st.subheader(f"Importar")

        if f'file_uploader_key_{table_name}' not in st.session_state:
            st.session_state[f'file_uploader_key_{table_name}'] = 0

        uploaded_file = st.file_uploader(
            f"⬆️ Carregar {title}.csv",
            type="csv",
            # Usa a key dinâmica do session_state
            key=f"upload_{table_name}_{st.session_state[f'file_uploader_key_{table_name}']}",
            accept_multiple_files=False
        )

        if uploaded_file is not None:
            try:
                csv_df = pd.read_csv(uploaded_file)
                
                # --- NOVO: Placeholder para o DataFrame (se você ainda não tem) ---
                dataframe_preview_placeholder = st.empty()
                dataframe_preview_placeholder.write("Pré-visualização do CSV carregado:")
                dataframe_preview_placeholder.dataframe(csv_df, use_container_width=True) # Display preview

                if st.button(f"✅ Confirmar Importação de {title}", key=f"confirm_import_{table_name}", use_container_width=True):
                    if db_manager.insert_dataframe_batch(table_name, csv_df, id_column_to_exclude=id_column):
                        st.success(f"Dados de {title} importados com sucesso!")
                        
                        # --- NOVO: Limpar o placeholder IMEDIATAMENTE ---
                        dataframe_preview_placeholder.empty() 
                        
                        time.sleep(2) # Pausa para ver a mensagem
                        
                        # --- NOVO: Incrementa a key do file_uploader para forçar o reset ---
                        st.session_state[f'file_uploader_key_{table_name}'] += 1
                        
                        st.rerun() # Recarrega a página

                    else:
                        st.error(f"Falha ao importar dados de {title}. Verifique o formato do CSV e as mensagens de erro.")
            except Exception as e:
                st.error(f"Erro ao ler ou processar o arquivo CSV: {e}\n"
                        "Verifique se o arquivo é um CSV válido e se as colunas correspondem ao esperado.")

        
    st.subheader(f"Adicionar novo {title[:-1]}")
    new_data = {}
    for col in columns:
        if col != id_column:
            new_data[col] = st.text_input(f"{col.replace('_', ' ').capitalize()} ({title[:-1]})", key=f"{table_name}_{col}_add")
    if st.button(f"Adicionar {title[:-1]}", key=f"add_{table_name}"):
        cols = ', '.join([col for col in columns if col != id_column])
        vals = ', '.join(['%s'] * (len(columns) - 1))
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"
        db_manager.execute_query(query, tuple(new_data[col] for col in columns if col != id_column))
        st.success(f"{title[:-1]} adicionado com sucesso!")
        st.rerun()
    st.subheader(f"Editar/Excluir {title[:-1]}")
    if df is not None and not df.empty:
        selected = st.selectbox(f"Selecione o {title[:-1]} para editar/excluir", df[id_column])
        selected_row = df[df[id_column] == selected].iloc[0]
        edit_data = {}
        for col in columns:
            if col != id_column:
                edit_data[col] = st.text_input(f"{col.replace('_', ' ').capitalize()} (editar)", value=str(selected_row[col]), key=f"{table_name}_{col}_edit")
        col1, col2 = st.columns(2)
        with col1:
            if st.button(f"Atualizar {title[:-1]}", key=f"update_{table_name}"):
                set_clause = ', '.join([f"{col} = %s" for col in columns if col != id_column])
                query = f"UPDATE {table_name} SET {set_clause} WHERE {id_column} = %s"
                db_manager.execute_query(query, tuple(edit_data[col] for col in columns if col != id_column) + (selected,))
                st.success(f"{title[:-1]} atualizado com sucesso!")
                st.rerun()
        with col2:
            if st.button(f"Excluir {title[:-1]}", key=f"delete_{table_name}"):
                query = f"DELETE FROM {table_name} WHERE {id_column} = %s"
                db_manager.execute_query(query, (selected,))
                st.success(f"{title[:-1]} excluído com sucesso!")
                st.rerun()

def show():
    st.title("Relatórios Dinâmicos")

    db_manager = Manage_database()
    
    crud_section(
        "Fornecedores",
        "tb_fornecedor",
        ["id_fornecedor", "nome_fornecedor", "cnpj_fornecedor", "telefone_fornecedor", "email_fornecedor", "endereco_fornecedor"],
        "id_fornecedor",
        db_manager
    )

    crud_section(
        "Produtos",
        "tb_produto",
        ["id_produto", "nome_produto", "tipo_corte", "unidade_medida", "preco_compra", "preco_venda", "id_fornecedor"],
        "id_produto",
        db_manager
    )

    crud_section(
        "Clientes",
        "tb_cliente",
        ["id_cliente", "nome_cliente", "cnpj_cliente", "endereco_cliente", "telefone_cliente", "email_cliente", "tipo_cliente"],
        "id_cliente",
        db_manager
    )