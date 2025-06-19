import streamlit as st

def show():
    st.sidebar.title("Menu")
    menu_options = ["Página Inicial", "Pedidos", "Clientes", "Produtos", "Relatórios", "Configurações"]
    escolha = st.sidebar.radio("Navegação", menu_options)

    st.image(
        "https://images.unsplash.com/photo-1504674900247-0877df9cc836?auto=format&fit=crop&w=800&q=80",
        use_column_width=True,
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

