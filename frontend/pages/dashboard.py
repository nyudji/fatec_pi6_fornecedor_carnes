import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import locale  # Importa módulo locale para formatação numérica

def show():
    from driver.psycopg2_connect import PostgresConnect

    st.set_page_config(layout="wide", page_title="Dashboard de Vendas de Carnes")

    # --- Configuração da Localização para pt_BR ---
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')  # Linux/macOS
    except locale.Error:
        try:
            locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')  # Windows
        except locale.Error:
            st.warning("Não foi possível configurar locale pt_BR. Formatação pode ficar incorreta.")
            locale.setlocale(locale.LC_ALL, '')  # Usa locale padrão
    
    # --- Função para carregar dados com cache ---
    @st.cache_data(ttl=3600)
    def load_data_from_db(query, table_name="dados"):
        db_connection = PostgresConnect()
        if db_connection.conn is None or db_connection.conn.closed:
            st.error(f"Erro ao conectar ao banco para carregar {table_name}.")
            return pd.DataFrame()
        try:
            df = pd.read_sql(query, db_connection.conn)
            return df
        except Exception as e:
            st.error(f"Erro na consulta {table_name}: {e}")
            return pd.DataFrame()
        finally:
            db_connection.close_connection()

    # --- Queries com adição do id_produto em pedidos detalhes ---
    QUERY_PEDIDOS_DETALHES = """
    SELECT
        p.id_pedido,
        p.data_pedido,
        p.status AS status_pedido,
        p.valor_total,
        c.nome_cliente,
        c.tipo_cliente,
        ip.id_item_pedido,
        ip.id_produto,  -- ADICIONADO para usar no cálculo do lucro
        prod.nome_produto,
        prod.tipo_corte,
        ip.quantidade,
        ip.unidade_medida,
        ip.preco_unitario
    FROM tb_pedido p
    JOIN tb_cliente c ON p.id_cliente = c.id_cliente
    JOIN tb_item_pedido ip ON p.id_pedido = ip.id_pedido
    JOIN tb_produto prod ON ip.id_produto = prod.id_produto
    ORDER BY p.data_pedido;
    """

    QUERY_CLIENTES = """
    SELECT
        id_cliente,
        nome_cliente,
        cnpj_cliente,
        telefone_cliente,
        email_cliente,
        endereco_cliente,
        tipo_cliente
    FROM tb_cliente;
    """

    QUERY_PRODUTOS = """
    SELECT
        id_produto,
        nome_produto,
        tipo_corte,
        unidade_medida,
        preco_compra,
        preco_venda,
        id_fornecedor
    FROM tb_produto;
    """

    QUERY_PAGAMENTOS = """
    SELECT
        pa.id_pagamento,
        pa.id_pedido,
        pa.data_pagamento,
        pa.valor_pago,
        pa.metodo_pagamento,
        pa.status AS status_pagamento,
        ped.data_pedido,
        ped.status AS status_pedido,
        ped.valor_total AS valor_total_pedido
    FROM tb_pagamento pa
    JOIN tb_pedido ped ON pa.id_pedido = ped.id_pedido
    ORDER BY pa.data_pagamento;
    """

    QUERY_ESTOQUE = """
    SELECT
        e.id_estoque,
        e.quantidade_disponivel,
        e.localizacao,
        pe.lote,
        pe.validade,
        p.nome_produto,
        p.tipo_corte,
        p.unidade_medida,
        p.preco_venda
    FROM tb_estoque e
    JOIN tb_produto_entrada pe ON e.item_entrada = pe.id_item_entrada
    JOIN tb_produto p ON pe.id_produto = p.id_produto
    ORDER BY p.nome_produto, pe.validade;
    """

    # --- Carrega os DataFrames ---
    df_pedidos_detalhes = load_data_from_db(QUERY_PEDIDOS_DETALHES, "pedidos e itens")
    df_clientes = load_data_from_db(QUERY_CLIENTES, "clientes")
    df_produtos = load_data_from_db(QUERY_PRODUTOS, "produtos")
    df_pagamentos = load_data_from_db(QUERY_PAGAMENTOS, "pagamentos")
    df_estoque = load_data_from_db(QUERY_ESTOQUE, "estoque")

    if (df_pedidos_detalhes.empty and
        df_clientes.empty and
        df_produtos.empty and
        df_pagamentos.empty and
        df_estoque.empty):
        st.warning("Nenhum dado foi carregado do banco. Verifique a conexão e população do banco.")
        st.stop()

    # --- Preparação dos dados ---
    if not df_pedidos_detalhes.empty:
        df_pedidos_detalhes['data_pedido'] = pd.to_datetime(df_pedidos_detalhes['data_pedido'])
        df_pedidos_detalhes['mes_ano_pedido'] = df_pedidos_detalhes['data_pedido'].dt.to_period('M').astype(str)

    if not df_pagamentos.empty:
        df_pagamentos['data_pagamento'] = pd.to_datetime(df_pagamentos['data_pagamento'])
        df_pagamentos['mes_ano_pagamento'] = df_pagamentos['data_pagamento'].dt.to_period('M').astype(str)

    # --- Cálculo do Lucro ---
    if not df_pedidos_detalhes.empty and not df_produtos.empty:
        df_pedidos_detalhes = df_pedidos_detalhes.merge(
            df_produtos[['id_produto', 'preco_compra']],
            on='id_produto',
            how='left'
        )
        df_pedidos_detalhes['lucro_item'] = df_pedidos_detalhes['quantidade'] * (
            df_pedidos_detalhes['preco_unitario'] - df_pedidos_detalhes['preco_compra']
        )
        df_pedidos_detalhes['ano_pedido'] = df_pedidos_detalhes['data_pedido'].dt.year
        lucro_anual = df_pedidos_detalhes.groupby('ano_pedido')['lucro_item'].sum().reset_index()
        lucro_medio_anual = lucro_anual['lucro_item'].mean() if not lucro_anual.empty else 0
        ano_atual = datetime.now().year
        lucro_ano_atual = lucro_anual.loc[lucro_anual['ano_pedido'] == ano_atual, 'lucro_item'].sum() if ano_atual in lucro_anual['ano_pedido'].values else 0
    else:
        lucro_medio_anual = 0
        lucro_ano_atual = 0
        ano_atual = datetime.now().year

    # --- KPI: Vendas Mensais (col6) ---
    mes_atual = datetime.now().month
    ano_atual = datetime.now().year
    data_atual = datetime(ano_atual, mes_atual, 1)

    # Define o mês anterior (lida com virada de ano)
    data_mes_passado = data_atual - pd.DateOffset(months=1)
    ano_passado = data_mes_passado.year
    mes_passado = data_mes_passado.month

    # Filtro e soma
    venda_mes_atual = df_pedidos_detalhes[
        (df_pedidos_detalhes['data_pedido'].dt.month == mes_atual) &
        (df_pedidos_detalhes['data_pedido'].dt.year == ano_atual)
    ].drop_duplicates('id_pedido')['valor_total'].sum()

    venda_mes_passado = df_pedidos_detalhes[
        (df_pedidos_detalhes['data_pedido'].dt.month == mes_passado) &
        (df_pedidos_detalhes['data_pedido'].dt.year == ano_passado)
    ].drop_duplicates('id_pedido')['valor_total'].sum()

    # Cálculo do delta e exibição personalizada
    delta_mensal = venda_mes_atual - venda_mes_passado
    simbolo_mes = "▼" if delta_mensal < 0 else ""
    texto_mes = "Abaixo do mês anterior" if delta_mensal < 0 else "Acima do mês anterior"
    cor_mes = "inverse" if delta_mensal < 0 else "off"  # vermelho apenas se negativo


    # --- KPIs Gerais ---
    st.title("📊 Dashboard de Vendas de Carnes")
    st.markdown("Uma visão geral dos dados populados do sistema de gerenciamento de carnes.")

    st.subheader("📈 KPIs Gerais")
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    if not df_pedidos_detalhes.empty:
        unique_pedidos_for_total = df_pedidos_detalhes.drop_duplicates(subset=['id_pedido'])
        total_vendas = unique_pedidos_for_total['valor_total'].sum()
    else:
        total_vendas = 0
    total_pedidos = df_pedidos_detalhes['id_pedido'].nunique() if not df_pedidos_detalhes.empty else 0
    media_valor_pedido = total_vendas / total_pedidos if total_pedidos > 0 else 0
    total_clientes = df_clientes['id_cliente'].nunique() if not df_clientes.empty else 0

    # Cálculo do delta para o lucro do ano atual em relação à média
    delta_lucro = lucro_ano_atual - lucro_medio_anual

    # Texto para explicar se está acima ou abaixo da média
    if delta_lucro < 0:
        texto_delta = "Abaixo da média"
        simbolo_delta = "▼"
    else:
        texto_delta = "Acima da média" if delta_lucro > 0 else "Na média"
        simbolo_delta = ""
        
    def format_currency_br(value):
        # Formata valores grandes em R$ 15,3M, R$ 1,2K, etc
        if abs(value) >= 1_000_000:
            return f"R$ {value/1_000_000:.1f}M"
        elif abs(value) >= 1_000:
            return f"R$ {value/1_000:.1f}K"
        else:
            return locale.currency(value, grouping=True, symbol='R$ ')
    # Exibir KPIs com formatação aprimorada
    with col1:
        
        st.metric("Total de Vendas", format_currency_br(total_vendas))
    with col2:
        st.metric("Total de Pedidos", f"{total_pedidos:n}")
    with col3:
        st.metric("Ticket Médio por Pedido", locale.currency(media_valor_pedido, grouping=True, symbol='R$ '))
    with col4:
        st.metric("Total de Clientes", f"{total_clientes:n}")
    with col5:
        st.metric("Vendas (Média Anual)", format_currency_br(lucro_medio_anual))
    # Exibir o KPI final
    with col6:
        st.metric(
            f"Vendas {data_atual.strftime('%B/%Y').capitalize()}",
            format_currency_br(venda_mes_atual),
            delta=f"{simbolo_mes} {format_currency_br(abs(delta_mensal))} ({texto_mes})",
            delta_color=cor_mes
        )
    st.markdown("---")

    # --- Visualizações ---

    # 1. Vendas ao Longo do Tempo
    st.subheader("Vendas e Pedidos ao Longo do Tempo")
    col_vendas_data, col_pedidos_data = st.columns(2)

    if not df_pedidos_detalhes.empty:
        # Agrupar vendas por data (using unique pedido totals if necessary for graph, or item totals)
        # For temporal trends, summing item prices (ip.quantidade * ip.preco_unitario) is often preferred
        # or ensuring the 'valor_total' from tb_pedido is used only once per date.
        
        # Option 1: Summing valor_total from unique pedidos for each date
        vendas_por_data = df_pedidos_detalhes.drop_duplicates(subset=['id_pedido']).groupby('data_pedido')['valor_total'].sum().reset_index()
        
        # Option 2: (Alternative if valor_total in tb_pedido is sum of items):
        # Calculate sales based on item prices if that's what you want to visualize daily details
        # df_pedidos_detalhes['valor_item_calculado'] = df_pedidos_detalhes['quantidade'] * df_pedidos_detalhes['preco_unitario']
        # vendas_por_data = df_pedidos_detalhes.groupby('data_pedido')['valor_item_calculado'].sum().reset_index()
        # Rename 'valor_item_calculado' to 'valor_total' for plot if you use this alternative

        fig_vendas_data = px.line(vendas_por_data, x='data_pedido', y='valor_total', 
                                title='Volume de Vendas Diárias', labels={'data_pedido': 'Data do Pedido', 'valor_total': 'Total de Vendas (R$)'})
        fig_vendas_data.update_xaxes(rangeselector_buttons=list([
            dict(count=7, label="1w", step="day", stepmode="backward"),
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all")
        ]))
        
        # Agrupar número de pedidos por data
        pedidos_por_data = df_pedidos_detalhes.groupby('data_pedido')['id_pedido'].nunique().reset_index(name='num_pedidos') # Count unique orders per day
        fig_pedidos_data = px.line(pedidos_por_data, x='data_pedido', y='num_pedidos', 
                                title='Número de Pedidos Diários', labels={'data_pedido': 'Data do Pedido', 'num_pedidos': 'Número de Pedidos'})
        fig_pedidos_data.update_xaxes(rangeselector_buttons=list([
            dict(count=7, label="1w", step="day", stepmode="backward"),
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all")
        ]))

        with col_vendas_data:
            st.plotly_chart(fig_vendas_data, use_container_width=True)
        with col_pedidos_data:
            st.plotly_chart(fig_pedidos_data, use_container_width=True)
    else:
        st.info("Dados de vendas ausentes para visualização temporal.")


    st.markdown("---")

    # 2. Análise de Produtos
    st.subheader("Análise de Produtos e Vendas")
    col_top_produtos, col_tipo_corte = st.columns(2)

    if not df_pedidos_detalhes.empty:
        # Top N Produtos Mais Vendidos (por quantidade)
        top_produtos_qtd = df_pedidos_detalhes.groupby('nome_produto')['quantidade'].sum().nlargest(10).reset_index()
        fig_top_produtos_qtd = px.bar(top_produtos_qtd, x='quantidade', y='nome_produto', 
                                    title='Top 10 Produtos Mais Vendidos (Quantidade)',
                                    orientation='h',
                                    labels={'quantidade': 'Quantidade Total Vendida', 'nome_produto': 'Produto'})
        fig_top_produtos_qtd.update_layout(yaxis={'categoryorder':'total ascending'}) # Ordena do menor para o maior
        with col_top_produtos:
            st.plotly_chart(fig_top_produtos_qtd, use_container_width=True)

        # Distribuição de Vendas por Tipo de Corte (summing valor_total of items)
        # For product analysis, sum of (quantidade * preco_unitario) is generally more accurate
        df_pedidos_detalhes['valor_item_calculado'] = df_pedidos_detalhes['quantidade'] * df_pedidos_detalhes['preco_unitario']
        vendas_por_corte = df_pedidos_detalhes.groupby('tipo_corte')['valor_item_calculado'].sum().reset_index()
        fig_vendas_por_corte = px.pie(vendas_por_corte, names='tipo_corte', values='valor_item_calculado',
                                    title='Distribuição de Vendas por Tipo de Corte',
                                    hole=0.3,
                                    labels={'valor_item_calculado': 'Valor Total de Itens (R$)'}) # Clarify label
        with col_tipo_corte:
            st.plotly_chart(fig_vendas_por_corte, use_container_width=True)
    else:
        st.info("Dados de produtos ou vendas ausentes para esta seção.")

    st.markdown("---")

    # 3. Análise de Clientes
    st.subheader("Análise de Clientes")
    col_tipo_cliente, col_top_clientes = st.columns(2)

    if not df_clientes.empty:
        # Distribuição de Clientes por Tipo
        dist_tipo_cliente = df_clientes['tipo_cliente'].value_counts().reset_index()
        dist_tipo_cliente.columns = ['Tipo de Cliente', 'Número de Clientes']
        fig_tipo_cliente = px.bar(dist_tipo_cliente, x='Tipo de Cliente', y='Número de Clientes',
                                title='Distribuição de Clientes por Tipo',
                                color='Tipo de Cliente')
        with col_tipo_cliente:
            st.plotly_chart(fig_tipo_cliente, use_container_width=True)

    if not df_pedidos_detalhes.empty:
        # Top N Clientes por Valor Total de Compras (using unique order totals for clients)
        # This ensures each client's total is based on their unique order values
        cliente_valor_total = df_pedidos_detalhes.drop_duplicates(subset=['id_pedido', 'nome_cliente']) # Ensure unique order per client
        top_clientes_valor = cliente_valor_total.groupby('nome_cliente')['valor_total'].sum().nlargest(10).reset_index()
        
        fig_top_clientes_valor = px.bar(top_clientes_valor, x='valor_total', y='nome_cliente',
                                    title='Top 10 Clientes por Valor de Compras',
                                    orientation='h',
                                    labels={'valor_total': 'Valor Total Comprado (R$)', 'nome_cliente': 'Cliente'})
        fig_top_clientes_valor.update_layout(yaxis={'categoryorder':'total ascending'})
        with col_top_clientes:
            st.plotly_chart(fig_top_clientes_valor, use_container_width=True)
    else:
        st.info("Dados de clientes ou pedidos ausentes para esta seção.")

    st.markdown("---")

    # 4. Análise de Pedidos (Status e Itens)
    st.subheader("Status dos Pedidos")

    if not df_pedidos_detalhes.empty:
        # Distribuição de Status de Pedido (count unique orders)
        status_counts = df_pedidos_detalhes.drop_duplicates(subset=['id_pedido'])['status_pedido'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Contagem']
        fig_status_pedido = px.pie(status_counts, names='Status', values='Contagem',
                                title='Distribuição de Status dos Pedidos',
                                hole=0.3)
        st.plotly_chart(fig_status_pedido, use_container_width=True)
    else:
        st.info("Dados de pedidos ausentes para esta seção.")

    st.markdown("---")

    # 5. Análise de Pagamentos
    st.subheader("Análise de Pagamentos")
    col_metodo_pagamento, col_status_pagamento, col_pagamento_valor_metodo = st.columns(3)

    if not df_pagamentos.empty:
        # Distribuição de Métodos de Pagamento
        metodo_pagamento_counts = df_pagamentos['metodo_pagamento'].value_counts().reset_index()
        metodo_pagamento_counts.columns = ['Método de Pagamento', 'Contagem']
        fig_metodo_pagamento = px.bar(metodo_pagamento_counts, x='Método de Pagamento', y='Contagem',
                                    title='Distribuição de Métodos de Pagamento',
                                    color='Método de Pagamento')
        with col_metodo_pagamento:
            st.plotly_chart(fig_metodo_pagamento, use_container_width=True)

        # Distribuição de Status de Pagamento
        status_pagamento_counts = df_pagamentos['status_pagamento'].value_counts().reset_index()
        status_pagamento_counts.columns = ['Status', 'Contagem']
        fig_status_pagamento = px.pie(status_pagamento_counts, names='Status', values='Contagem',
                                    title='Distribuição de Status de Pagamento',
                                    hole=0.3)
        with col_status_pagamento:
            st.plotly_chart(fig_status_pagamento, use_container_width=True)

        # Valor Pago por Método de Pagamento
        valor_por_metodo = df_pagamentos.groupby('metodo_pagamento')['valor_pago'].sum().reset_index()
        fig_valor_por_metodo = px.bar(valor_por_metodo, x='metodo_pagamento', y='valor_pago',
                                    title='Valor Total Pago por Método de Pagamento',
                                    labels={'metodo_pagamento': 'Método de Pagamento', 'valor_pago': 'Valor Pago (R$)'},
                                    color='metodo_pagamento')
        with col_pagamento_valor_metodo:
            st.plotly_chart(fig_valor_por_metodo, use_container_width=True)
    else:
        st.info("Dados de pagamentos ausentes para esta seção.")

    st.markdown("---")

    # 6. Análise de Estoque
    st.subheader("Análise de Estoque")

    if not df_estoque.empty:
        col_estoque_produto, col_estoque_validade = st.columns(2)

        # Quantidade de Estoque por Produto
        estoque_por_produto = df_estoque.groupby('nome_produto')['quantidade_disponivel'].sum().nlargest(10).reset_index()
        fig_estoque_produto = px.bar(estoque_por_produto, x='quantidade_disponivel', y='nome_produto',
                                    title='Produtos mais estocados',
                                    orientation='h',
                                    labels={'quantidade_disponivel': 'Quantidade Disponível', 'nome_produto': 'Produto'})
        fig_estoque_produto.update_layout(yaxis={'categoryorder':'total ascending'})
        with col_estoque_produto:
            st.plotly_chart(fig_estoque_produto, use_container_width=True)

        # Estoque por Validade (apenas para ilustrar, pode ser mais complexo)
        # Converta para datetime e agrupe por mês/ano da validade
        df_estoque['validade'] = pd.to_datetime(df_estoque['validade'])
        estoque_por_validade = df_estoque.groupby(df_estoque['validade'].dt.to_period('M'))['quantidade_disponivel'].sum().reset_index()
        estoque_por_validade['validade'] = estoque_por_validade['validade'].astype(str) # Para visualização no Plotly
        
        fig_estoque_validade = px.bar(estoque_por_validade, x='validade', y='quantidade_disponivel',
                                    title='Estoque por Mês de Validade',
                                    labels={'validade': 'Mês de Validade', 'quantidade_disponivel': 'Quantidade Disponível'})
        fig_estoque_validade.update_xaxes(categoryorder='category ascending') # Garante a ordem cronológica
        with col_estoque_validade:
            st.plotly_chart(fig_estoque_validade, use_container_width=True)
    else:
        st.info("Dados de estoque ausentes para esta seção.")

    st.markdown("---")
    st.write("Dados atualizados automaticamente. Última atualização: " + datetime.now().strftime("%H:%M:%S"))