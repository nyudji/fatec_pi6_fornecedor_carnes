from models.database_psycopg_manager import Manage_database
from frontend.pages import relatorios, dashboard

# Import Libs
import streamlit as st

# Instancia a classe de gerenciamento do banco de dados
db_manager = Manage_database()

st.set_page_config(page_title="Mini ERP - Distribuidora de Carnes", layout="wide")

# Menu lateral para navegação entre as páginas/tabelas
st.sidebar.title("Menu")
# menu_options = [
#     "Página Inicial",
#     "Fornecedores",
#     "Produtos",
#     "Clientes",
#     "Pedidos",
#     "Dashboard",
#     "Configurações"
# ]

menu_options = [
    "Página Inicial",
    "Fornecedores",
    "Produtos",
    "Clientes",
    "Relatórios",
    "Dashboard"
]

escolha = st.sidebar.radio("Navegação", menu_options)

if escolha == "Página Inicial":
    st.image(
        "https://images.unsplash.com/photo-1504674900247-0877df9cc836?auto=format&fit=crop&w=800&q=80",
        use_container_width=True,
        caption="Mini ERP - Distribuidora de Carnes"
    )
    st.markdown("""
    # Bem-vindo ao Mini ERP - Distribuidora de Carnes 🥩

    Este sistema foi desenvolvido para facilitar a gestão de pedidos, clientes, produtos e relatórios de sua distribuidora de carnes.

    **Funcionalidades principais:**
    - Cadastro e consulta de fornecedores, clientes e produtos
    - Controle de estoque e entradas de mercadorias
    - Gestão de pedidos e pagamentos
    - Relatórios dinâmicos para tomada de decisão

    ---
    """)
    st.info("Utilize o menu lateral para navegar entre as funcionalidades do sistema.")
    st.markdown("""
    <div style='text-align: center; color: gray; font-size: 12px;'>
        Desenvolvido por sua equipe de TI • 2025
    </div>
    """, unsafe_allow_html=True)

elif escolha == "Fornecedores":
    relatorios.crud_section(
        "Fornecedores",
        "tb_fornecedor",
        ["id_fornecedor", "nome_fornecedor", "cnpj_fornecedor", "telefone_fornecedor", "email_fornecedor", "endereco_fornecedor"],
        "id_fornecedor",
        db_manager
    )

elif escolha == "Produtos":
    relatorios.crud_section(
        "Produtos",
        "tb_produto",
        ["id_produto", "nome_produto", "tipo_corte", "unidade_medida", "preco_compra", "preco_venda", "id_fornecedor"],
        "id_produto",
        db_manager
    )

elif escolha == "Clientes":
    relatorios.crud_section(
        "Clientes",
        "tb_cliente",
        ["id_cliente", "nome_cliente", "cnpj_cliente", "endereco_cliente", "telefone_cliente", "email_cliente", "tipo_cliente"],
        "id_cliente",
        db_manager
    )

elif escolha == "Relatórios":
    relatorios.show()

elif escolha == "Dashboard":
    dashboard.show()

elif escolha == "Pedidos":
    st.info("Funcionalidade de Pedidos em desenvolvimento.")

elif escolha == "Configurações":
    st.info("Funcionalidade de Configurações em desenvolvimento.")