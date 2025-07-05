import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import locale  # Importa módulo locale para formatação numérica

# Importa a classe de conexão com o banco de dados
from driver.psycopg2_connect import PostgresConnect


def format_currency_br(value):
    """
    Formata valores numéricos para o formato monetário brasileiro.
    Lida com valores grandes (Milhões, Milhares) e formatação padrão.
    """
    try:
        if abs(value) >= 1_000_000:
            return f"R$ {value / 1_000_000:.1f}M"
        elif abs(value) >= 1_000:
            return f"R$ {value / 1_000:.1f}K"
        else:
            # Tenta usar o locale para formatação de moeda
            return locale.currency(value, grouping=True, symbol='R$ ')
    except TypeError:  # Lida com casos onde o valor pode não ser numérico
        return str(value)
    except Exception:  # Fallback para qualquer outro erro do locale
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


@st.cache_data(ttl=3600)
def load_data_from_db(query, table_name="dados"):
    """
    Carrega dados do banco de dados PostgreSQL com cache.
    """
    db_connection = PostgresConnect()
    if db_connection.conn is None or db_connection.conn.closed:
        st.error(f"Erro ao conectar ao banco para carregar {table_name}. Verifique as credenciais e o status do DB.")
        return pd.DataFrame()
    try:
        df = pd.read_sql(query, db_connection.conn)
        return df
    except Exception as e:
        st.error(f"Erro na consulta {table_name}: {e}")
        return pd.DataFrame()
    finally:
        db_connection.close_connection()


def prepare_data(df_pedidos_detalhes, df_clientes, df_produtos, df_pagamentos, df_estoque):
    """
    Prepara e limpa os DataFrames, calculando métricas adicionais como lucro.
    """
    # Converter colunas de data para datetime
    if not df_pedidos_detalhes.empty:
        df_pedidos_detalhes['data_pedido'] = pd.to_datetime(df_pedidos_detalhes['data_pedido'])
        df_pedidos_detalhes['mes_ano_pedido'] = df_pedidos_detalhes['data_pedido'].dt.to_period('M').astype(str)

    if not df_pagamentos.empty:
        df_pagamentos['data_pagamento'] = pd.to_datetime(df_pagamentos['data_pagamento'])
        df_pagamentos['mes_ano_pagamento'] = df_pagamentos['data_pagamento'].dt.to_period('M').astype(str)

    if not df_estoque.empty:
        df_estoque['validade'] = pd.to_datetime(df_estoque['validade'])

    # Cálculo do Lucro (merge com preco_compra)
    if not df_pedidos_detalhes.empty and not df_produtos.empty:
        # Garante que 'id_produto' é do mesmo tipo em ambos os DFs para o merge
        df_pedidos_detalhes['id_produto'] = df_pedidos_detalhes['id_produto'].astype(str)
        df_produtos['id_produto'] = df_produtos['id_produto'].astype(str)

        df_pedidos_detalhes = df_pedidos_detalhes.merge(
            df_produtos[['id_produto', 'preco_compra']],
            on='id_produto',
            how='left'
        )
        # Calcula lucro_item apenas onde preco_compra não é nulo para evitar NaNs
        df_pedidos_detalhes['lucro_item'] = df_pedidos_detalhes.apply(
            lambda row: row['quantidade'] * (row['preco_unitario'] - row['preco_compra'])
            if pd.notna(row['preco_compra']) else 0, axis=1
        )
        df_pedidos_detalhes['ano_pedido'] = df_pedidos_detalhes['data_pedido'].dt.year
    else:
        df_pedidos_detalhes['lucro_item'] = 0
        df_pedidos_detalhes['ano_pedido'] = datetime.now().year  # Garante a coluna mesmo que vazia

    # Calcular valor_item_calculado para análises de produto
    if not df_pedidos_detalhes.empty:
        df_pedidos_detalhes['valor_item_calculado'] = df_pedidos_detalhes['quantidade'] * df_pedidos_detalhes[
            'preco_unitario']

    return df_pedidos_detalhes, df_clientes, df_produtos, df_pagamentos, df_estoque


def display_kpis(df_pedidos_detalhes, df_clientes):
    """
    Exibe os KPIs gerais do dashboard.
    """
    st.subheader("📈 KPIs Gerais")
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    # Total de Vendas
    total_vendas = 0
    if not df_pedidos_detalhes.empty:
        unique_pedidos_for_total = df_pedidos_detalhes.drop_duplicates(subset=['id_pedido'])
        total_vendas = unique_pedidos_for_total['valor_total'].sum()

    # Total de Pedidos
    total_pedidos = df_pedidos_detalhes['id_pedido'].nunique() if not df_pedidos_detalhes.empty else 0

    # Ticket Médio
    media_valor_pedido = total_vendas / total_pedidos if total_pedidos > 0 else 0

    # Total de Clientes
    total_clientes = df_clientes['id_cliente'].nunique() if not df_clientes.empty else 0

    # Lucro Anual e Médio Anual
    lucro_anual = df_pedidos_detalhes.groupby('ano_pedido')[
        'lucro_item'].sum().reset_index() if not df_pedidos_detalhes.empty else pd.DataFrame()
    lucro_medio_anual = lucro_anual['lucro_item'].mean() if not lucro_anual.empty else 0
    ano_atual = datetime.now().year
    lucro_ano_atual = lucro_anual.loc[lucro_anual['ano_pedido'] == ano_atual, 'lucro_item'].sum() if ano_atual in \
                                                                                                     lucro_anual[
                                                                                                         'ano_pedido'].values else 0
    #lucro semanal
    today_date = datetime.now().date()

    # Calculo de  e nício e fim da semana (segunda-feira a domingo) para a data atual do sistema
    start_of_current_week = today_date - timedelta(days=today_date.weekday())
    end_of_current_week = start_of_current_week + timedelta(days=6)
    df_pedidos_apenas_data = df_pedidos_detalhes.copy()  # Criar uma cópia para garantir que não haja modificações no DF original
    df_pedidos_apenas_data['data_pedido_date_only'] = df_pedidos_apenas_data['data_pedido'].dt.date

    df_current_week = df_pedidos_apenas_data[
        (df_pedidos_apenas_data['data_pedido_date_only'] >= start_of_current_week) &
        (df_pedidos_apenas_data['data_pedido_date_only'] <= end_of_current_week)
        ]
    lucro_semanal = df_current_week['lucro_item'].sum()

    # Cálculo do delta para o lucro do ano atual em relação à média
    delta_lucro = lucro_ano_atual - lucro_medio_anual

    # Texto e símbolo para o delta
    texto_delta = "Na média"
    simbolo_delta = ""
    cor_delta = "off"
    if delta_lucro < 0:
        texto_delta = "Abaixo da média"
        simbolo_delta = "▼"
        cor_delta = "inverse"
    elif delta_lucro > 0:
        texto_delta = "Acima da média"
        simbolo_delta = "▲"  # Alterado para triângulo para cima
        cor_delta = "normal"

    with col1:
        st.metric("Total de Vendas", format_currency_br(total_vendas))
    with col2:
        st.metric("Total de Pedidos", f"{total_pedidos:n}")
    with col3:
        st.metric("Ticket Médio por Pedido", format_currency_br(media_valor_pedido))
    with col4:
        st.metric("Total de Clientes", f"{total_clientes:n}")
    with col5:
        st.metric("Lucro Médio", format_currency_br(lucro_medio_anual))
    with col6:
        st.metric(
            f"Lucro Semanal ({start_of_current_week.strftime('%d/%m')} - {end_of_current_week.strftime('%d/%m')})",
            format_currency_br(lucro_semanal)
        )
    st.markdown("---")


def display_sales_trends(df_pedidos_detalhes, start_date, end_date):
    """
    Exibe gráficos de vendas e pedidos ao longo do tempo com granularidade selecionável e métricas.
    """
    st.subheader("Vendas e Pedidos ao Longo do Tempo")

    # Filtrar o DataFrame com base no período selecionado na barra lateral
    df_filtered_by_date = df_pedidos_detalhes[
        (df_pedidos_detalhes['data_pedido'].dt.date >= start_date) &
        (df_pedidos_detalhes['data_pedido'].dt.date <= end_date)
        ].copy()

    if df_filtered_by_date.empty:
        st.info("Nenhum dado de vendas encontrado para o período selecionado para visualização temporal.")
        st.markdown("---")
        return


    # --- Seleção da Granularidade ---
    st.markdown("##### Frequência dos Gráficos")
    granularidade = st.radio(
        "Selecione a frequência para os gráficos:",
        ('Diária', 'Semanal', 'Mensal', 'Anual'),
        index=2,  # Padrão para Mensal
        key="sales_trend_granularity"
    )

    # Definir a frequência com base na granularidade
    freq_map = {
        'Diária': 'D',
        'Semanal': 'W-MON',  # Começa a semana na segunda-feira
        'Mensal': 'ME',  # Final do mês
        'Anual': 'YE'  # Final do ano
    }
    selected_freq = freq_map[granularidade]

    # Cores personalizadas para as linhas
    sales_line_color = '#1f77b4'  # Azul
    orders_line_color = '#ff7f0e'  # Laranja

    col_vendas_data, col_pedidos_data = st.columns(2)

    # --- Gráfico de Volume de Vendas ao Longo do Tempo ---
    with col_vendas_data:
        vendas_por_data = df_filtered_by_date.drop_duplicates(subset=['id_pedido']).groupby(
            pd.Grouper(key='data_pedido', freq=selected_freq)
        )['valor_total'].sum().reset_index()

        # O Grouper pode retornar algumas datas sem vendas no período, Plotly lida bem com isso
        # Se quiser remover, use: vendas_por_data = vendas_por_data[vendas_por_data['valor_total'] > 0]

        fig_vendas_data = px.line(
            vendas_por_data,
            x='data_pedido',
            y='valor_total',
            title=f'Volume de Vendas {granularidade}',
            labels={'data_pedido': f'Data ({granularidade})', 'valor_total': 'Total de Vendas (R$)'},
            line_shape="linear",  # ou "spline" para curvas mais suaves
            color_discrete_sequence=[sales_line_color]  # Aplica a cor definida
        )
        fig_vendas_data.update_layout(yaxis_tickprefix="R$ ")
        fig_vendas_data.update_traces(
            mode='lines+markers',  # Mostra linhas e marcadores
            hovertemplate='<b>Data:</b> %{x|%d/%m/%Y}<br><b>Vendas:</b> %{y:,.2f}<extra></extra>'
            # Formato para o tooltip
        )
        fig_vendas_data.update_xaxes(rangeselector_buttons=list([
            dict(count=7, label="1w", step="day", stepmode="backward"),
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all")
        ]))
        st.plotly_chart(fig_vendas_data, use_container_width=True)

    # --- Gráfico de Número de Pedidos ao Longo do Tempo ---
    with col_pedidos_data:
        pedidos_por_data = df_filtered_by_date.groupby(
            pd.Grouper(key='data_pedido', freq=selected_freq)
        )['id_pedido'].nunique().reset_index(name='num_pedidos')

        fig_pedidos_data = px.line(
            pedidos_por_data,
            x='data_pedido',
            y='num_pedidos',
            title=f'Número de Pedidos {granularidade}',
            labels={'data_pedido': f'Data ({granularidade})', 'num_pedidos': 'Número de Pedidos'},
            line_shape="linear",
            color_discrete_sequence=[orders_line_color]  # Aplica a cor definida
        )
        fig_pedidos_data.update_traces(
            mode='lines+markers',
            hovertemplate='<b>Data:</b> %{x|%d/%m/%Y}<br><b>Pedidos:</b> %{y}<extra></extra>'
        )
        fig_pedidos_data.update_xaxes(rangeselector_buttons=list([
            dict(count=7, label="1w", step="day", stepmode="backward"),
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all")
        ]))
        st.plotly_chart(fig_pedidos_data, use_container_width=True)

    st.markdown("---")


def display_product_analysis(df_pedidos_detalhes, df_produtos):
    """
    Exibe análises detalhadas de produtos e vendas, incluindo rentabilidade e tendências.
    Aprimorado com melhores cores e visualização para gráficos de rosca.
    """
    st.subheader("Análise de Produtos e Vendas")

    if df_pedidos_detalhes.empty:
        st.info("Nenhum dado de vendas disponível para análise de produtos.")
        st.markdown("---")
        return

    # Certifique-se de que 'valor_item_calculado' e 'lucro_item' existem,
    # que são calculados na função prepare_data
    if 'valor_item_calculado' not in df_pedidos_detalhes.columns:
        st.warning("Coluna 'valor_item_calculado' não encontrada. Verifique a função 'prepare_data'.")
        return
    if 'lucro_item' not in df_pedidos_detalhes.columns:
        st.warning("Coluna 'lucro_item' não encontrada. Verifique a função 'prepare_data'.")
        return

    # --- Filtro para Top Produtos/Lucro ---
    col_filters, _ = st.columns([0.3, 0.7])
    with col_filters:
        all_cut_types = sorted(df_pedidos_detalhes['tipo_corte'].unique())
        selected_cut_type = st.multiselect(
            "Filtrar por Tipo de Corte (para Top Produtos/Lucro):",
            options=['Todos'] + all_cut_types,
            default='Todos',
            key="product_analysis_cut_type_filter"
        )

    df_filtered_by_cut_type = df_pedidos_detalhes.copy()
    if 'Todos' not in selected_cut_type and selected_cut_type:
        df_filtered_by_cut_type = df_pedidos_detalhes[df_pedidos_detalhes['tipo_corte'].isin(selected_cut_type)]

    if df_filtered_by_cut_type.empty:
        st.info("Nenhum dado de vendas encontrado para os tipos de corte selecionados.")
        st.markdown("---")
        return

    # --- Paleta de Cores para Tipos de Corte (se necessário, defina aqui para consistência) ---

    cut_type_colors = {
    "Picanha": "#B00020",  # Vermelho vinho intenso (Riqueza)
    "Filé mignon": "#8B0000", # Vermelho escuro, quase bordô (Premium)
    "Ancho": "#A0522D",     # Marrom Rosado (Corte específico, sabor)
    "Chorizo": "#CD5C5C",   # Vermelho Indiano (Apetite)
    "Contra filé": "#DC143C", # Carmim (Forte, Saboroso)
    "Maminha": "#B22222",   # Tijolo (Cor de carne assada)
    "Alcatra": "#8B4513",   # Sela Marrom (Corte versátil)
    "Costela": "#6A0505",   # Marrom avermelhado escuro (Carne cozida lentamente)
    "Cupim": "#5C4033",     # Marrom Café (Sabor robusto)
    "Fraldinha": "#FF6347", # Tomate (Fresco, suculento)
    "Lagarto": "#A9A9A9",   # Cinza escuro (Neutro, para cortes de cozimento lento)
    "Lombo": "#D2691E",     # Chocolate (Suíno, caramelizado)
    "Cordeiro": "#4B0082",  # Índigo (Diferenciado, sabor único)
    "Panceta": "#FFD700",   # Ouro (Crocrante, sabor intenso)
    "Linguiça": "#FF8C00",  # Laranja Escuro (Artesanal, sabor)
    "Filé suíno": "#FF4500",# Laranja Vermelho (Sabor, maciez)

    # Cores para outros tipos (tons que complementam, menos intensos ou mais neutros)
    "Tulipa": "#FFA07A",    # Salmão Claro
    "Barriga": "#8B4513",   # Sela Marrom (repetida, se não houver conflito nas maiores fatias)
    "Patinho": "#6B8E23",   # Verde Oliva Escuro (Natural, fresco)
    "Coxão mole": "#CD853F",# Peru (Neutro, versátil)
    "Músculo": "#D2B48C",   # Tan (Textura)
    "Acém": "#F4A460",      # Areia Escura
    "Rabo": "#A52A2A",      # Marrom avermelhado
    "Bisteca": "#BC8F8F", # Rosado (Corte rápido)
    "Joelho": "#F0E68C",    # Cáqui (Corte de cozimento)
    "Kafta": "#6B8E23",     # Verde Oliva (Tempero, ervas)
    "Salame": "#DCDCDC",    # Cinza Claro (Processado)
    "Banha": "#FFFACD",     # Limão Chiffon (Claro, gordura)
    "Pé": "#FAEBD7",        # Branco Antigo (Neutro)
    "Pele": "#FFFAF0",      # Floral White (Claro)
    "Manta de pernil": "#808080", # Cinza (Versátil)
    "Short rib": "#2F4F4F", # Cinza Escuro (Textura)
    "T-bone": "#8B008B",    # Dark Magenta (Único, especial)
    "Prime rib": "#BDB76B", # Verde Oliva Escuro
    "Tomahawk": "#8FBC8F",  # Verde Claro
    "Denver": "#DCDCDC",    # Cinza Claro
    "Baby beef": "#F5DEB3", # Trigo (Cor mais clara)
    "Shoulder": "#9370DB",  # Roxo Médio (Diferente)
    "Entranha": "#3CB371",  # Verde Média (Fresco)
    "Brisket": "#B0C4DE",   # Azul Claro (Fumaça)
    "Manta bovina": "#DDA0DD", # Orquídea Clara (Diferente)
    "Linguiça cuiabana": "#BC8F8F", # Rosado
    "Linguiça mista": "#8B4513", # Sela Marrom
    "Chuleta": "#8B0000",   # Escuro (Intenso)
    "Outros": "#696969"
    }

    col_top_produtos_qtd, col_top_produtos_lucro = st.columns(2)

    # --- 1. Top N Produtos Mais Vendidos (por quantidade) ---
    with col_top_produtos_qtd:
        top_produtos_qtd = df_filtered_by_cut_type.groupby('nome_produto')['quantidade'].sum().nlargest(
            10).reset_index()
        fig_top_produtos_qtd = px.bar(top_produtos_qtd, x='quantidade', y='nome_produto',
                                      title='Top 10 Produtos Mais Vendidos (Quantidade)',
                                      orientation='h',
                                      labels={'quantidade': 'Quantidade Total Vendida', 'nome_produto': 'Produto'},
                                      color_discrete_sequence=px.colors.qualitative.Plotly
                                      # Paleta de cores para barras
                                      )
        fig_top_produtos_qtd.update_layout(yaxis={'categoryorder': 'total ascending'})
        fig_top_produtos_qtd.update_traces(
            hovertemplate='<b>Produto:</b> %{y}<br><b>Quantidade:</b> %{x}<extra></extra>')
        st.plotly_chart(fig_top_produtos_qtd, use_container_width=True)

    # --- 2. Top N Produtos por Lucro ---
    with col_top_produtos_lucro:
        top_produtos_lucro = df_filtered_by_cut_type.groupby('nome_produto')['lucro_item'].sum().nlargest(
            10).reset_index()
        fig_top_produtos_lucro = px.bar(top_produtos_lucro, x='lucro_item', y='nome_produto',
                                        title='Top 10 Produtos Mais Lucrativos (R$)',
                                        orientation='h',
                                        labels={'lucro_item': 'Lucro Total (R$)', 'nome_produto': 'Produto'},
                                        color_discrete_sequence=px.colors.qualitative.Bold  # Outra paleta de cores
                                        )
        fig_top_produtos_lucro.update_layout(yaxis={'categoryorder': 'total ascending'})
        fig_top_produtos_lucro.update_layout(xaxis_tickprefix="R$ ")
        fig_top_produtos_lucro.update_traces(
            hovertemplate='<b>Produto:</b> %{y}<br><b>Lucro:</b> %{x:,.2f} R$<extra></extra>')
        st.plotly_chart(fig_top_produtos_lucro, use_container_width=True)

    st.markdown("---")

    col_vendas_corte, col_lucro_corte = st.columns(2)

    # --- 3. Distribuição de Vendas por Tipo de Corte ---
    with col_vendas_corte:
        vendas_por_corte = df_pedidos_detalhes.groupby('tipo_corte')['valor_item_calculado'].sum().reset_index()

        # Melhoria para gráficos de rosca: agrupar fatias pequenas em "Outros"
        # Isso é útil se tivermos muitos tipos de corte com valores muito pequenos
        total_vendas_corte = vendas_por_corte['valor_item_calculado'].sum()
        min_percent_display = 2.0  # Mostrar fatias maiores que 2%
        vendas_por_corte['percentage'] = (vendas_por_corte['valor_item_calculado'] / total_vendas_corte) * 100

        # Cria uma nova linha 'Outros' para as categorias pequenas
        outros_vendas = vendas_por_corte[vendas_por_corte['percentage'] < min_percent_display]
        principais_vendas = vendas_por_corte[vendas_por_corte['percentage'] >= min_percent_display]

        if not outros_vendas.empty:
            outros_sum = outros_vendas['valor_item_calculado'].sum()
            principais_vendas = pd.concat([principais_vendas, pd.DataFrame(
                [{'tipo_corte': 'Outros', 'valor_item_calculado': outros_sum,
                  'percentage': (outros_sum / total_vendas_corte) * 100}])])

        # Ordenar para que "Outros" fique no final ou onde desejar
        principais_vendas = principais_vendas.sort_values(by='valor_item_calculado', ascending=False)
        if 'Outros' in principais_vendas['tipo_corte'].values:
            outros_row = principais_vendas[principais_vendas['tipo_corte'] == 'Outros']
            principais_vendas = principais_vendas[principais_vendas['tipo_corte'] != 'Outros']
            principais_vendas = pd.concat([principais_vendas, outros_row])

        fig_vendas_por_corte = px.pie(principais_vendas, names='tipo_corte', values='valor_item_calculado',
                                      title='Distribuição de Vendas por Tipo de Corte',
                                      hole=0.3,
                                      labels={'valor_item_calculado': 'Valor Total de Itens (R$)'},
                                      color='tipo_corte',  # Mapeia cor por tipo de corte
                                      color_discrete_map=cut_type_colors if cut_type_colors else px.colors.qualitative.Bold,
                                      # Usa mapa se definido, senão padrão
                                      category_orders={"tipo_corte": principais_vendas['tipo_corte'].tolist()}
                                      # Garante a ordem
                                      )
        fig_vendas_por_corte.update_traces(
            textinfo='percent+label',  # Exibe percentual e rótulo na fatia
            hovertemplate='<b>Tipo de Corte:</b> %{label}<br><b>Valor:</b> %{value:,.2f} R$ (%{percent})<extra></extra>',
            textfont_size=12,  # Ajusta tamanho da fonte do texto na fatia
            marker=dict(line=dict(color='#000000', width=1))  # Adiciona borda preta para melhor distinção
        )
        # Se for um número muito grande de categorias, considere transformar em gráfico de barras
        if len(principais_vendas) > 10:  # Limite arbitrário, ajuste conforme seu caso
            fig_vendas_por_corte = px.bar(principais_vendas, x='tipo_corte', y='valor_item_calculado',
                                          title='Distribuição de Vendas por Tipo de Corte',
                                          labels={'tipo_corte': 'Tipo de Corte',
                                                  'valor_item_calculado': 'Valor Total de Itens (R$)'},
                                          color='tipo_corte',
                                          color_discrete_map=cut_type_colors if cut_type_colors else px.colors.qualitative.Bold,
                                          height=400 + len(principais_vendas) * 20
                                          # Ajusta altura para não ficar apertado
                                          )
            fig_vendas_por_corte.update_layout(xaxis={'categoryorder': 'total descending'})
            fig_vendas_por_corte.update_layout(yaxis_tickprefix="R$ ")
            fig_vendas_por_corte.update_traces(
                hovertemplate='<b>Tipo de Corte:</b> %{x}<br><b>Valor:</b> %{y:,.2f} R$<extra></extra>')

        st.plotly_chart(fig_vendas_por_corte, use_container_width=True)

    # --- 4. Distribuição de Lucro por Tipo de Corte ---
    with col_lucro_corte:
        lucro_por_corte = df_pedidos_detalhes.groupby('tipo_corte')['lucro_item'].sum().reset_index()

        # Melhoria para gráficos de rosca: agrupar fatias pequenas em "Outros"
        total_lucro_corte = lucro_por_corte['lucro_item'].sum()
        lucro_por_corte['percentage'] = (lucro_por_corte['lucro_item'] / total_lucro_corte) * 100

        outros_lucro = lucro_por_corte[lucro_por_corte['percentage'] < min_percent_display]
        principais_lucro = lucro_por_corte[lucro_por_corte['percentage'] >= min_percent_display]

        if not outros_lucro.empty:
            outros_sum_lucro = outros_lucro['lucro_item'].sum()
            principais_lucro = pd.concat([principais_lucro, pd.DataFrame(
                [{'tipo_corte': 'Outros', 'lucro_item': outros_sum_lucro,
                  'percentage': (outros_sum_lucro / total_lucro_corte) * 100}])])

        principais_lucro = principais_lucro.sort_values(by='lucro_item', ascending=False)
        if 'Outros' in principais_lucro['tipo_corte'].values:
            outros_row_lucro = principais_lucro[principais_lucro['tipo_corte'] == 'Outros']
            principais_lucro = principais_lucro[principais_lucro['tipo_corte'] != 'Outros']
            principais_lucro = pd.concat([principais_lucro, outros_row_lucro])

        fig_lucro_por_corte = px.pie(principais_lucro, names='tipo_corte', values='lucro_item',
                                     title='Distribuição de Lucro por Tipo de Corte',
                                     hole=0.3,
                                     labels={'lucro_item': 'Lucro Total (R$)'},
                                     color='tipo_corte',  # Mapeia cor por tipo de corte
                                     color_discrete_map=cut_type_colors if cut_type_colors else px.colors.qualitative.Pastel,
                                     # Usa mapa se definido, senão padrão
                                     category_orders={"tipo_corte": principais_lucro['tipo_corte'].tolist()}
                                     )
        fig_lucro_por_corte.update_traces(
            textinfo='percent+label',
            hovertemplate='<b>Tipo de Corte:</b> %{label}<br><b>Lucro:</b> %{value:,.2f} R$ (%{percent})<extra></extra>',
            textfont_size=12,
            marker=dict(line=dict(color='#000000', width=1))
        )
        # Se for um número muito grande de categorias, considere transformar em gráfico de barras
        if len(principais_lucro) > 10:  # Limite arbitrário
            fig_lucro_por_corte = px.bar(principais_lucro, x='tipo_corte', y='lucro_item',
                                         title='Distribuição de Lucro por Tipo de Corte',
                                         labels={'tipo_corte': 'Tipo de Corte', 'lucro_item': 'Lucro Total (R$)'},
                                         color='tipo_corte',
                                         color_discrete_map=cut_type_colors if cut_type_colors else px.colors.qualitative.Pastel,
                                         height=400 + len(principais_lucro) * 20
                                         )
            fig_lucro_por_corte.update_layout(xaxis={'categoryorder': 'total descending'})
            fig_lucro_por_corte.update_layout(yaxis_tickprefix="R$ ")
            fig_lucro_por_corte.update_traces(
                hovertemplate='<b>Tipo de Corte:</b> %{x}<br><b>Lucro:</b> %{y:,.2f} R$<extra></extra>')

        st.plotly_chart(fig_lucro_por_corte, use_container_width=True)

    st.markdown("---")

    # --- 5. Tendência de Vendas de Produtos Individuais ---
    st.markdown("### Tendência de Vendas de Produtos Individuais")

    # Obter lista de produtos vendidos no período filtrado
    available_products = sorted(df_pedidos_detalhes['nome_produto'].unique())
    selected_product_for_trend = st.selectbox(
        "Selecione um produto para ver sua tendência de vendas:",
        options=['Selecione um produto'] + available_products,
        key="product_trend_selector"
    )

    if selected_product_for_trend != 'Selecione um produto':
        df_product_trend = df_pedidos_detalhes[df_pedidos_detalhes['nome_produto'] == selected_product_for_trend].copy()

        if not df_product_trend.empty:
            product_sales_trend = df_product_trend.groupby(pd.Grouper(key='data_pedido', freq='ME')).agg(
                Quantidade_Vendida=('quantidade', 'sum'),
                Valor_Vendido=('valor_item_calculado', 'sum'),
                Lucro_Gerado=('lucro_item', 'sum')
            ).reset_index()

            metric_to_display = st.radio(
                "Mostrar tendência por:",
                ('Quantidade Vendida', 'Valor Vendido', 'Lucro Gerado'),
                key="product_trend_metric_radio"
            )

            y_column = ''
            y_title = ''
            hover_format = ''
            if metric_to_display == 'Quantidade Vendida':
                y_column = 'Quantidade_Vendida'
                y_title = 'Quantidade Total Vendida'
                hover_format = '%{y}'
            elif metric_to_display == 'Valor Vendido':
                y_column = 'Valor_Vendido'
                y_title = 'Valor Total Vendido (R$)'
                hover_format = 'R$ %{y:,.2f}'
            else:  # Lucro Gerado
                y_column = 'Lucro_Gerado'
                y_title = 'Lucro Total Gerado (R$)'
                hover_format = 'R$ %{y:,.2f}'

            fig_product_trend = px.line(
                product_sales_trend,
                x='data_pedido',
                y=y_column,
                title=f"Tendência de {metric_to_display} para {selected_product_for_trend}",
                labels={'data_pedido': 'Mês/Ano', y_column: y_title},
                markers=True,
                color_discrete_sequence=[px.colors.qualitative.Dark2[0]]  # Cor única para a linha
            )
            if 'Valor' in y_title or 'Lucro' in y_title:
                fig_product_trend.update_layout(yaxis_tickprefix="R$ ")
            fig_product_trend.update_traces(
                hovertemplate=f'<b>Período:</b> %{{x|%Y-%m}}<br><b>{metric_to_display}:</b> {hover_format}<extra></extra>')
            fig_product_trend.update_xaxes(rangeselector_buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all")
            ]))
            st.plotly_chart(fig_product_trend, use_container_width=True)
        else:
            st.info(f"Nenhuma venda encontrada para '{selected_product_for_trend}' no período filtrado.")
    else:
        st.info("Selecione um produto para visualizar a tendência de vendas.")

    st.markdown("---")

    # --- 6. Tabela Detalhada de Itens de Pedido ---
    st.markdown("### Detalhes dos Itens de Pedido Vendidos")

    # Adicionar filtro para tipo de corte na tabela detalhada
    all_table_cut_types = sorted(df_pedidos_detalhes['tipo_corte'].unique())
    selected_table_cut_type = st.multiselect(
        "Filtrar itens da tabela por Tipo de Corte:",
        options=['Todos'] + all_table_cut_types,
        default='Todos',
        key="product_item_table_cut_type_filter"
    )

    df_itens_tabela = df_pedidos_detalhes.copy()
    if 'Todos' not in selected_table_cut_type and selected_table_cut_type:
        df_itens_tabela = df_itens_tabela[df_itens_tabela['tipo_corte'].isin(selected_table_cut_type)]

    # Adicionar filtro para status do pedido na tabela detalhada
    all_table_status = sorted(df_pedidos_detalhes['status_pedido'].unique())
    selected_table_status = st.multiselect(
        "Filtrar itens da tabela por Status do Pedido:",
        options=['Todos'] + all_table_status,
        default=['Faturado', 'Entregue'],  # Exemplo: padrão para pedidos concluídos
        key="product_item_table_status_filter"
    )

    if 'Todos' not in selected_table_status and selected_table_status:
        df_itens_tabela = df_itens_tabela[df_itens_tabela['status_pedido'].isin(selected_table_status)]

    if not df_itens_tabela.empty:
        # Colunas a serem exibidas na tabela
        columns_to_display = [
            'data_pedido', 'nome_cliente', 'nome_produto', 'tipo_corte',
            'quantidade', 'unidade_medida', 'preco_unitario', 'valor_item_calculado', 'lucro_item', 'status_pedido'
        ]

        st.dataframe(
            df_itens_tabela[columns_to_display].rename(columns={
                'data_pedido': 'Data',
                'nome_cliente': 'Cliente',
                'nome_produto': 'Produto',
                'tipo_corte': 'Tipo de Corte',
                'quantidade': 'Qtd.',
                'unidade_medida': 'Unid.',
                'preco_unitario': 'Preço Unitário',
                'valor_item_calculado': 'Valor Total Item',
                'lucro_item': 'Lucro Item',
                'status_pedido': 'Status Pedido'
            }),
            use_container_width=True,
            column_config={
                "Data": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "Preço Unitário": st.column_config.NumberColumn(format="R$ %.2f"),
                "Valor Total Item": st.column_config.NumberColumn(format="R$ %.2f"),
                "Lucro Item": st.column_config.NumberColumn(format="R$ %.2f"),
            },
            hide_index=True
        )
    else:
        st.info("Nenhum item de pedido encontrado para os filtros selecionados.")
    st.markdown("---")

def display_client_analysis(df_clientes, df_pedidos_detalhes):
    """
    Exibe análises detalhadas de clientes, incluindo distribuição por tipo e top clientes.
    """
    st.subheader("Análise de Clientes")

    # Garante que temos apenas uma entrada por pedido para o valor_total do pedido
    # e que id_pedido e nome_cliente são únicos para a soma
    df_pedidos_unicos_para_clientes = df_pedidos_detalhes.drop_duplicates(subset=['id_pedido', 'nome_cliente'])

    col_kpi_clientes1, col_kpi_clientes2 = st.columns(2)
    with col_kpi_clientes1:
        total_clientes = df_clientes['id_cliente'].nunique() if not df_clientes.empty else 0
        st.metric("Total de Clientes Cadastrados", f"{total_clientes:n}")
    with col_kpi_clientes2:
        if not df_pedidos_unicos_para_clientes.empty:
            total_clientes_compradores = df_pedidos_unicos_para_clientes['nome_cliente'].nunique()
            st.metric("Clientes com Pedidos Registrados", f"{total_clientes_compradores:n}")
        else:
            st.metric("Clientes com Pedidos Registrados", "N/A")
    st.markdown("---")

    col_tipo_cliente, col_top_clientes_valor = st.columns(2)

    if not df_clientes.empty:
        # --- 1. Distribuição de Clientes por Tipo ---
        dist_tipo_cliente = df_clientes['tipo_cliente'].value_counts().reset_index()
        dist_tipo_cliente.columns = ['Tipo de Cliente', 'Número de Clientes']

        # Mapeamento de cores para tipos de cliente (exemplo)
        tipo_cliente_colors = {
            "Atacado": "#1f77b4",  # Azul
            "Varejo": "#ff7f0e",  # Laranja
            "Restaurante": "#2ca02c",  # Verde
            "Mercado": "#d62728"  # Vermelho
        }

        fig_tipo_cliente = px.bar(
            dist_tipo_cliente,
            x='Tipo de Cliente',
            y='Número de Clientes',
            title='Distribuição de Clientes por Tipo',
            color='Tipo de Cliente',
            color_discrete_map=tipo_cliente_colors
        )
        fig_tipo_cliente.update_traces(hovertemplate='<b>Tipo:</b> %{x}<br><b>Contagem:</b> %{y}<extra></extra>')
        with col_tipo_cliente:
            st.plotly_chart(fig_tipo_cliente, use_container_width=True)
    else:
        col_tipo_cliente.info("Dados de clientes ausentes para distribuição por tipo.")

    if not df_pedidos_detalhes.empty:
        # --- 2. Top N Clientes por Valor Total de Compras ---
        top_clientes_valor = df_pedidos_unicos_para_clientes.groupby('nome_cliente')['valor_total'].sum().nlargest(
            10).reset_index()

        fig_top_clientes_valor = px.bar(
            top_clientes_valor,
            x='valor_total',
            y='nome_cliente',
            title='Top 10 Clientes por Valor de Compras',
            orientation='h',
            labels={'valor_total': 'Valor Total Comprado (R$)', 'nome_cliente': 'Cliente'},
            color='valor_total',  # Adiciona gradiente de cor pelo valor
            color_continuous_scale=px.colors.sequential.Plasma  # Escolhe uma escala de cores
        )
        fig_top_clientes_valor.update_layout(yaxis={'categoryorder': 'total ascending'})
        fig_top_clientes_valor.update_layout(xaxis_tickprefix="R$ ")
        fig_top_clientes_valor.update_traces(
            hovertemplate='<b>Cliente:</b> %{y}<br><b>Valor Total:</b> %{x:,.2f} R$<extra></extra>')

        with col_top_clientes_valor:
            st.plotly_chart(fig_top_clientes_valor, use_container_width=True)
    else:
        col_top_clientes_valor.info("Dados de pedidos ausentes para top clientes.")

    st.markdown("---")

    # --- 3. Valor Total de Vendas por Tipo de Cliente ---
    st.markdown("### Valor de Vendas por Tipo de Cliente")
    if not df_pedidos_unicos_para_clientes.empty:
        vendas_por_tipo_cliente = df_pedidos_unicos_para_clientes.groupby('tipo_cliente')[
            'valor_total'].sum().reset_index()
        fig_vendas_por_tipo = px.pie(
            vendas_por_tipo_cliente,
            names='tipo_cliente',
            values='valor_total',
            title='Faturamento por Tipo de Cliente',
            hole=0.3,
            color='tipo_cliente',
            color_discrete_map=tipo_cliente_colors  # Reutiliza as cores dos tipos de cliente
        )
        fig_vendas_por_tipo.update_traces(textinfo='percent+label',
                                          hovertemplate='<b>Tipo:</b> %{label}<br><b>Valor:</b> %{value:,.2f} R$ (%{percent})<extra></extra>')
        st.plotly_chart(fig_vendas_por_tipo, use_container_width=True)
    else:
        st.info("Nenhum dado de vendas por tipo de cliente disponível.")

    st.markdown("---")

    # --- 4. Tabela Detalhada de Clientes ---
    st.markdown("### Detalhes dos Clientes")

    if not df_clientes.empty:
        # Filtro de tipo de cliente para a tabela detalhada
        all_client_types = sorted(df_clientes['tipo_cliente'].unique())
        selected_client_type_for_table = st.multiselect(
            "Filtrar clientes por tipo:",
            options=all_client_types,
            default=all_client_types,
            key="client_type_table_filter"
        )

        df_clientes_para_tabela = df_clientes[df_clientes['tipo_cliente'].isin(selected_client_type_for_table)]

        if not df_clientes_para_tabela.empty:
            st.dataframe(
                df_clientes_para_tabela[[
                    'nome_cliente', 'tipo_cliente', 'telefone_cliente', 'email_cliente', 'endereco_cliente'
                ]].rename(columns={
                    'nome_cliente': 'Nome',
                    'tipo_cliente': 'Tipo',
                    'telefone_cliente': 'Telefone',
                    'email_cliente': 'Email',
                    'endereco_cliente': 'Endereço'
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Nenhum cliente encontrado para os tipos selecionados.")
    else:
        st.info("Nenhum dado de cliente para exibir.")
    st.markdown("---")


def display_order_status(df_pedidos_detalhes):
    """
    Exibe a análise e o status dos pedidos com mais detalhes e interatividade.
    """
    st.subheader("Status dos Pedidos")

    if not df_pedidos_detalhes.empty:
        # Garante que temos apenas uma entrada por pedido para o status
        df_pedidos_unicos = df_pedidos_detalhes.drop_duplicates(subset=['id_pedido']).copy()

        # --- 1. Filtro de Status ---
        st.markdown("##### Filtrar por Status do Pedido")
        all_status = df_pedidos_unicos['status_pedido'].unique()
        selected_status_filter = st.multiselect(
            "Selecione um ou mais status para visualizar:",
            options=sorted(all_status),
            default=sorted(all_status),  # Seleciona todos por padrão
            key="status_order_multiselect"
        )

        if not selected_status_filter:
            st.info("Por favor, selecione ao menos um status para visualizar os dados.")
            return

        df_filtered_by_status = df_pedidos_unicos[df_pedidos_unicos['status_pedido'].isin(selected_status_filter)]

        if df_filtered_by_status.empty:
            st.info("Nenhum pedido encontrado para os status selecionados no período filtrado.")
            return

        col_pie, col_metrics = st.columns(2)

        # --- Cores para Status ---
        status_colors = {
            "Pendente": "#ffc107",  # Amarelo/Laranja - Atenção
            "Faturado": "#17a2b8",  # Azul claro - Em processo
            "Entregue": "#28a745",  # Verde - Sucesso
            "Cancelado": "#dc3545"  # Vermelho - Alerta/Falha
        }

        # --- 2. Distribuição de Status de Pedido (Gráfico de Pizza) ---
        with col_pie:
            status_counts = df_filtered_by_status['status_pedido'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Contagem']
            fig_status_pedido = px.pie(
                status_counts,
                names='Status',
                values='Contagem',
                title='Distribuição de Status dos Pedidos',
                hole=0.3,
                color='Status',
                color_discrete_map=status_colors
            )
            fig_status_pedido.update_traces(textinfo='percent+label',
                                            hovertemplate='<b>Status:</b> %{label}<br><b>Contagem:</b> %{value} (%{percent})<extra></extra>')
            st.plotly_chart(fig_status_pedido, use_container_width=True)

        # --- 3. Métricas de Desempenho por Status ---
        with col_metrics:
            st.markdown("##### Métricas por Status")
            status_summary = df_filtered_by_status.groupby('status_pedido').agg(
                Total_Pedidos=('id_pedido', 'nunique'),
                Valor_Total=('valor_total', 'sum')
            ).reset_index().rename(columns={'status_pedido': 'Status'})

            if not status_summary.empty:
                for index, row in status_summary.iterrows():
                    status = row['Status']
                    total_pedidos = row['Total_Pedidos']
                    valor_total = row['Valor_Total']

                    # Adiciona um ícone ou emoji baseado no status
                    icon = ""
                    if status == "Entregue":
                        icon = "✅"
                    elif status == "Pendente":
                        icon = "⏳"
                    elif status == "Faturado":
                        icon = "📦"
                    elif status == "Cancelado":
                        icon = "❌"

                    st.markdown(
                        f"**{icon} {status}:** {total_pedidos} pedidos (Valor: {format_currency_br(valor_total)})")
            else:
                st.info("Nenhuma métrica para os status selecionados.")

        st.markdown("---")

        # --- 4. Visualização Temporal do Status (Exemplo: Pedidos Pendentes vs. Entregues ao longo do tempo) ---
        st.markdown("### Tendência de Status ao Longo do Tempo")

        # Filtro para um período específico, se necessário, ou usar o período global
        # Aqui vamos usar o 'df_pedidos_detalhes' global para a tendência, mas você pode adaptar
        # para usar o df_pedidos_filtrado da barra lateral se quiser.
        # Para este exemplo, vou considerar que o df_pedidos_detalhes já veio pré-filtrado pela data global.

        # Agrupa por mês/ano e status para ver a evolução
        status_temporal = df_pedidos_detalhes.drop_duplicates(subset=['id_pedido']).groupby([
            pd.Grouper(key='data_pedido', freq='ME'),
            'status_pedido'
        ])['id_pedido'].nunique().reset_index(name='Contagem')

        # Filtra apenas os status selecionados no multiselect
        status_temporal = status_temporal[status_temporal['status_pedido'].isin(selected_status_filter)]

        if not status_temporal.empty:
            fig_temporal_status = px.line(
                status_temporal,
                x='data_pedido',
                y='Contagem',
                color='status_pedido',
                title='Contagem de Pedidos por Status ao Longo do Tempo',
                labels={'data_pedido': 'Mês/Ano', 'Contagem': 'Número de Pedidos', 'status_pedido': 'Status'},
                color_discrete_map=status_colors,
                markers=True  # Adiciona marcadores para cada ponto
            )
            fig_temporal_status.update_layout(xaxis_title="Data", yaxis_title="Número de Pedidos")
            fig_temporal_status.update_xaxes(rangeselector_buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all")
            ]))
            st.plotly_chart(fig_temporal_status, use_container_width=True)
        else:
            st.info("Nenhum dado de tendência de status para o período e status selecionados.")

        st.markdown("---")

        # --- 5. Tabela Detalhada dos Pedidos ---
        st.markdown("### Detalhes dos Pedidos por Status")
        st.dataframe(
            df_filtered_by_status[[
                'id_pedido', 'data_pedido', 'nome_cliente', 'valor_total', 'status_pedido'
            ]].rename(columns={
                'id_pedido': 'ID Pedido',
                'data_pedido': 'Data do Pedido',
                'nome_cliente': 'Cliente',
                'valor_total': 'Valor Total',
                'status_pedido': 'Status'
            }),
            use_container_width=True,
            column_config={
                "Valor Total": st.column_config.NumberColumn(format="R$ %.2f"),
                "Data do Pedido": st.column_config.DateColumn(format="DD/MM/YYYY")
            },
            hide_index=True
        )

    else:
        st.info("Dados de pedidos ausentes para a análise de status.")
    st.markdown("---")

def display_payment_analysis(df_pagamentos):
    """
    Exibe análises relacionadas a pagamentos com mais opções de cores.
    """
    st.subheader("Análise de Pagamentos")
    col_metodo_pagamento, col_status_pagamento, col_pagamento_valor_metodo = st.columns(3)

    if not df_pagamentos.empty:
        # --- 1. Distribuição de Métodos de Pagamento ---
        metodo_pagamento_counts = df_pagamentos['metodo_pagamento'].value_counts().reset_index()
        metodo_pagamento_counts.columns = ['Método de Pagamento', 'Contagem']

        # Definir uma paleta de cores para métodos de pagamento
        # Você pode usar paletas pré-definidas do Plotly (e.g., 'Pastel', 'Dark2', 'Set3')
        # ou criar um dicionário de mapeamento de cores para consistência
        metodo_cores = {
            "PIX": "#1f77b4",        # Azul
            "Cartão de crédito": "#ff7f0e", # Laranja
            "Cartão de débito": "#2ca02c", # Verde
            "Boleto bancário": "#d62728", # Vermelho
            "Transferência bancária (TED)": "#9467bd" # Roxo
        }

        fig_metodo_pagamento = px.bar(
            metodo_pagamento_counts,
            x='Método de Pagamento',
            y='Contagem',
            title='Distribuição de Métodos de Pagamento',
            color='Método de Pagamento',
            color_discrete_map=metodo_cores # Aplica o mapeamento de cores
        )
        fig_metodo_pagamento.update_traces(hovertemplate='<b>Método:</b> %{x}<br><b>Contagem:</b> %{y}<extra></extra>')
        fig_metodo_pagamento.update_layout(xaxis_title="Método de Pagamento", yaxis_title="Número de Pagamentos")
        with col_metodo_pagamento:
            st.plotly_chart(fig_metodo_pagamento, use_container_width=True)

        # --- 2. Distribuição de Status de Pagamento ---
        status_pagamento_counts = df_pagamentos['status_pagamento'].value_counts().reset_index()
        status_pagamento_counts.columns = ['Status', 'Contagem']

        # Definir uma paleta de cores para status de pagamento
        status_cores = {
            "Pago": "#28a745",              # Verde (Sucesso)
            "Aguardando pagamento": "#ffc107", # Amarelo (Atenção)
            "Cancelado": "#dc3545"          # Vermelho (Alerta)
        }

        fig_status_pagamento = px.pie(
            status_pagamento_counts,
            names='Status',
            values='Contagem',
            title='Distribuição de Status de Pagamento',
            hole=0.3,
            color='Status', # Mapeia cores pelo status
            color_discrete_map=status_cores # Aplica o mapeamento de cores
        )
        fig_status_pagamento.update_traces(textinfo='percent+label', hovertemplate='<b>Status:</b> %{label}<br><b>Contagem:</b> %{value} (%{percent})<extra></extra>')
        with col_status_pagamento:
            st.plotly_chart(fig_status_pagamento, use_container_width=True)

        # --- 3. Valor Total Pago por Método de Pagamento ---
        valor_por_metodo = df_pagamentos.groupby('metodo_pagamento')['valor_pago'].sum().reset_index()

        # Reutilizar o mesmo mapeamento de cores dos métodos de pagamento para consistência
        fig_valor_por_metodo = px.bar(
            valor_por_metodo,
            x='metodo_pagamento',
            y='valor_pago',
            title='Valor Total Pago por Método de Pagamento',
            labels={'metodo_pagamento': 'Método de Pagamento', 'valor_pago': 'Valor Pago (R$)'},
            color='metodo_pagamento', # Mapeia cores pelo método
            color_discrete_map=metodo_cores # Reutiliza o mapeamento de cores
        )
        fig_valor_por_metodo.update_layout(yaxis_tickprefix="R$ ")
        fig_valor_por_metodo.update_traces(hovertemplate='<b>Método:</b> %{x}<br><b>Valor Pago:</b> %{y:,.2f} R$<extra></extra>')
        fig_valor_por_metodo.update_layout(xaxis_title="Método de Pagamento", yaxis_title="Valor Pago (R$)")
        with col_pagamento_valor_metodo:
            st.plotly_chart(fig_valor_por_metodo, use_container_width=True)
    else:
        st.info("Dados de pagamentos ausentes para esta seção.")
    st.markdown("---")


def display_stock_analysis(df_estoque, df_produtos):
    """
    Exibe análises relacionadas ao estoque com um seletor de data específico.
    """
    st.subheader("Análise de Estoque")

    if not df_estoque.empty:
        # --- Adicionar Seletor de Data para Análise de Validade do Estoque ---
        st.markdown("##### Filtrar Estoque por Período de Validade")

        # Encontra as datas mínimas e máximas de validade disponíveis nos dados
        min_validade_available = df_estoque['validade'].min().date()
        max_validade_available = df_estoque['validade'].max().date()

        col_date_start, col_date_end = st.columns(2)
        with col_date_start:
            # Garante que a data de início não seja maior que a data de fim no valor padrão
            default_start_validade = max(min_validade_available,
                                         datetime.now().date() - timedelta(days=365))  # Ex: último ano
            selected_start_validade = st.date_input(
                "Início da Validade",
                value=default_start_validade,
                min_value=min_validade_available,
                max_value=max_validade_available,
                key="stock_start_date"
            )
        with col_date_end:
            selected_end_validade = st.date_input(
                "Fim da Validade",
                value=max_validade_available,
                min_value=min_validade_available,
                max_value=max_validade_available,
                key="stock_end_date"
            )

        # Validação para garantir que a data de início não seja posterior à data de fim
        if selected_start_validade > selected_end_validade:
            st.error(
                "A 'Data de Início da Validade' não pode ser posterior à 'Data de Fim da Validade'. Por favor, ajuste as datas.")
            return  # Sai da função para evitar erros nos cálculos subsequentes

        # Filtrar o DataFrame de estoque com base nas datas selecionadas
        df_estoque_filtered = df_estoque[
            (df_estoque['validade'].dt.date >= selected_start_validade) &
            (df_estoque['validade'].dt.date <= selected_end_validade)
            ].copy()  # Use .copy() para evitar SettingWithCopyWarning

        if df_estoque_filtered.empty:
            st.info("Nenhum item de estoque encontrado para o período de validade selecionado.")
            return

        col_estoque_produto, col_estoque_validade = st.columns(2)

        # Quantidade de Estoque por Produto (usando df_estoque_filtered)
        estoque_por_produto = df_estoque_filtered.groupby('nome_produto')['quantidade_disponivel'].sum().nlargest(
            10).reset_index()
        fig_estoque_produto = px.bar(estoque_por_produto, x='quantidade_disponivel', y='nome_produto',
                                     title='Top 10 Produtos Mais Estocados (no período filtrado)',
                                     orientation='h',
                                     labels={'quantidade_disponivel': 'Quantidade Disponível',
                                             'nome_produto': 'Produto'})
        fig_estoque_produto.update_layout(yaxis={'categoryorder': 'total ascending'})
        fig_estoque_produto.update_traces(
            hovertemplate='<b>Produto:</b> %{y}<br><b>Quantidade:</b> %{x}<extra></extra>')
        with col_estoque_produto:
            st.plotly_chart(fig_estoque_produto, use_container_width=True)

        # Estoque por Validade (aplica-se a df_estoque_filtered)
        hoje = pd.to_datetime(datetime.now().date())
        df_estoque_filtered['dias_para_vencer'] = (df_estoque_filtered['validade'] - hoje).dt.days

        bins = [-float('inf'), 30, 60, float('inf')]
        labels = [ 'Vence em até 30 dias', 'Vence em 30-60 dias', 'Vence em mais de 60 dias']
        # Usar df_estoque_filtered aqui para o corte
        df_estoque_filtered['status_validade'] = pd.cut(df_estoque_filtered['dias_para_vencer'], bins=bins,
                                                        labels=labels, right=False, ordered=False)

        # Resumo da validade (usando df_estoque_filtered)
        # AQUI ESTÁ A CORREÇÃO: Adicionando o parâmetro 'observed=False'
        # e garantindo que 'status_validade' seja uma categoria para Plotly não reclamar
        resumo_validade = df_estoque_filtered.groupby('status_validade', observed=False)[
            'quantidade_disponivel'].sum().reset_index()
        # Garante a ordem das categorias no gráfico
        resumo_validade['status_validade'] = pd.Categorical(resumo_validade['status_validade'], categories=labels,
                                                            ordered=True)
        resumo_validade = resumo_validade.sort_values('status_validade')

        mapa_cores = {

            'Vence em até 30 dias': '#f0ad4e',  # Laranja
            'Vence em 30-60 dias': '#5bc0de',  # Azul claro
            'Vence em mais de 60 dias': '#5cb85c'  # Verde
        }
        fig_estoque_validade = px.bar(
            resumo_validade,
            x='status_validade',
            y='quantidade_disponivel',
            title='Visão Geral do Estoque por Status de Validade (no período filtrado)',
            labels={'status_validade': 'Status da Validade', 'quantidade_disponivel': 'Quantidade Total em Estoque'},
            color='status_validade',
            color_discrete_map=mapa_cores,
            text='quantidade_disponivel'
        )
        fig_estoque_validade.update_layout(showlegend=False)
        fig_estoque_validade.update_traces(
            hovertemplate='<b>Status:</b> %{x}<br><b>Quantidade:</b> %{y}<extra></extra>')
        with col_estoque_validade:
            st.plotly_chart(fig_estoque_validade, use_container_width=True)

        # --- 3. Permitir Análise Detalhada (Drill-down) ---
        st.write("---")
        st.markdown("### Investigar Produtos por Status de Validade")

        status_selecionado = st.selectbox(
            "Selecione um status para ver os produtos detalhados:",
            options=labels,
            index=1,  # Deixar "Vence em até 30 dias" como padrão
            key="status_validade_detail"  # Chave única para o widget
        )

        # Usa df_estoque_filtered para o drill-down
        df_detalhes_validade = df_estoque_filtered[
            df_estoque_filtered['status_validade'] == status_selecionado].sort_values('dias_para_vencer')

        if not df_detalhes_validade.empty:
            st.dataframe(
                df_detalhes_validade[['nome_produto', 'quantidade_disponivel', 'validade', 'dias_para_vencer']],
                use_container_width=True,
                column_config={
                    "nome_produto": "Produto",
                    "quantidade_disponivel": "Quantidade",
                    "validade": st.column_config.DateColumn("Data de Validade", format="DD/MM/YYYY"),
                    "dias_para_vencer": st.column_config.NumberColumn("Dias Para Vencer")
                },
                hide_index=True
            )
        else:
            st.success(f"Nenhum produto encontrado com o status: '{status_selecionado}' no período selecionado.")

    else:
        st.info("Dados de estoque ausentes para esta seção.")
    st.markdown("---")


def display_products_by_cut_type(df_produtos):
    """
    Exibe a tabela de produtos com filtro por tipo de corte e métricas de margem.
    """
    st.header("Análise de Produtos por Tipo de Corte")
    if not df_produtos.empty:
        tipos_corte = df_produtos['tipo_corte'].unique()
        tipo_selecionado = st.selectbox(
            'Selecione um tipo de corte para analisar:',
            ['Todos'] + sorted(list(tipos_corte)),
            key='filtro_corte_tabela_unica'
        )

        df_exibir = df_produtos.copy()
        if tipo_selecionado != 'Todos':
            df_exibir = df_exibir[df_exibir['tipo_corte'] == tipo_selecionado]

        st.write("---")
        col1, col2, col3 = st.columns(3)
        num_produtos = df_exibir['nome_produto'].nunique()
        preco_medio = df_exibir['preco_venda'].mean()
        preco_max = df_exibir['preco_venda'].max()

        col1.metric("Produtos Únicos", f"{num_produtos}")
        col2.metric("Preço Médio de Venda", format_currency_br(preco_medio))
        col3.metric("Preço Mais Alto", format_currency_br(preco_max))
        st.write("---")

        df_exibir['Margem (R$)'] = df_exibir['preco_venda'] - df_exibir['preco_compra']
        df_exibir['Margem (%)'] = 0.0
        mask = df_exibir['preco_venda'] > 0
        df_exibir.loc[mask, 'Margem (%)'] = (df_exibir['Margem (R$)'] / df_exibir['preco_venda']) * 100
        df_exibir['Valor (R$)'] = df_exibir['preco_venda']

        st.markdown(f"**Produtos para: {tipo_selecionado}**")
        st.dataframe(
            df_exibir,
            use_container_width=True,
            column_config={
                "nome_produto": "Produto",
                "preco_compra": st.column_config.NumberColumn("Preço Compra (R$)", format="R$ %.2f"),
                "Valor (R$)": st.column_config.NumberColumn("Preço Venda (R$)", format="R$ %.2f"),
                "preco_venda": st.column_config.BarChartColumn("Visualização Preço",
                                                               help="Comparação visual do preço de venda."),
                "Margem (R$)": st.column_config.NumberColumn("Margem (R$)", format="R$ %.2f"),
                "Margem (%)": st.column_config.NumberColumn("Margem de Lucro (%)",
                                                            help="A margem de lucro percentual de cada produto."),
                "tipo_corte": "Tipo de Corte",  # Exibir tipo de corte também na tabela
                "id_produto": None,
                "id_fornecedor": None
            },
            column_order=[
                "nome_produto", "tipo_corte",
                "preco_compra", "Valor (R$)", "Margem (R$)", "Margem (%)",
                "preco_venda"  # Manter para a visualização da barra, mesmo que oculta
            ],
            hide_index=True
        )
    else:
        st.info("Nenhum produto encontrado.")
    st.markdown("---")


# --- Funções de Queries (mantidas as originais) ---
QUERY_PEDIDOS_DETALHES = """
                         SELECT p.id_pedido, \
                                p.data_pedido, \
                                p.status AS status_pedido, \
                                p.valor_total, \
                                c.nome_cliente, \
                                c.tipo_cliente, \
                                ip.id_item_pedido, \
                                ip.id_produto, \
                                prod.nome_produto, \
                                prod.tipo_corte, \
                                ip.quantidade, \
                                ip.unidade_medida, \
                                ip.preco_unitario
                         FROM tb_pedido p
                                  JOIN tb_cliente c ON p.id_cliente = c.id_cliente
                                  JOIN tb_item_pedido ip ON p.id_pedido = ip.id_pedido
                                  JOIN tb_produto prod ON ip.id_produto = prod.id_produto
                         ORDER BY p.data_pedido; \
                         """

QUERY_CLIENTES = """
                 SELECT id_cliente, \
                        nome_cliente, \
                        cnpj_cliente, \
                        telefone_cliente, \
                        email_cliente, \
                        endereco_cliente, \
                        tipo_cliente
                 FROM tb_cliente; \
                 """

QUERY_PRODUTOS = """
                 SELECT id_produto, \
                        nome_produto, \
                        tipo_corte, \
                        unidade_medida, \
                        preco_compra, \
                        preco_venda, \
                        id_fornecedor
                 FROM tb_produto; \
                 """

QUERY_PAGAMENTOS = """
                   SELECT pa.id_pagamento, \
                          pa.id_pedido, \
                          pa.data_pagamento, \
                          pa.valor_pago, \
                          pa.metodo_pagamento, \
                          pa.status       AS status_pagamento, \
                          ped.data_pedido, \
                          ped.status      AS status_pedido, \
                          ped.valor_total AS valor_total_pedido
                   FROM tb_pagamento pa
                            JOIN tb_pedido ped ON pa.id_pedido = ped.id_pedido
                   ORDER BY pa.data_pagamento; \
                   """

QUERY_ESTOQUE = """
                SELECT e.id_estoque, \
                       e.quantidade_disponivel, \
                       e.localizacao, \
                       pe.lote, \
                       pe.validade, \
                       p.nome_produto, \
                       p.tipo_corte, \
                       p.unidade_medida, \
                       p.preco_venda
                FROM tb_estoque e
                         JOIN tb_produto_entrada pe ON e.item_entrada = pe.id_item_entrada
                         JOIN tb_produto p ON pe.id_produto = p.id_produto
                ORDER BY p.nome_produto, pe.validade; \
                """


def show():
    st.set_page_config(layout="wide", page_title="Dashboard de Vendas de Carnes")

    # --- Configuração da Localização para pt_BR ---
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
        except locale.Error as e:
            st.warning(f"Não foi possível configurar locale pt_BR. Formatação numérica pode ficar incorreta. Erro: {e}")
            locale.setlocale(locale.LC_ALL, '')
        except Exception as e:
            st.warning(
                f"Um erro inesperado ocorreu ao configurar o locale: {e}. Formatação numérica pode ficar incorreta.")
            locale.setlocale(locale.LC_ALL, '')
    except Exception as e:
        st.warning(f"Um erro inesperado ocorreu ao configurar o locale: {e}. Formatação numérica pode ficar incorreta.")
        locale.setlocale(locale.LC_ALL, '')

    st.title("📊 Dashboard de Vendas de Carnes")
    st.markdown("Uma visão geral dos dados populados do sistema de gerenciamento de carnes.")

    # --- Carrega os DataFrames ---
    with st.spinner("Carregando dados do banco de dados..."):
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

    # --- Preparação e Cálculos Iniciais ---
    df_pedidos_detalhes, df_clientes, df_produtos, df_pagamentos, df_estoque = \
        prepare_data(df_pedidos_detalhes, df_clientes, df_produtos, df_pagamentos, df_estoque)

    # --- Barra Lateral para Filtros Globais ---
    st.sidebar.header("Filtros de Período")
    min_date_available = df_pedidos_detalhes[
        'data_pedido'].min().date() if not df_pedidos_detalhes.empty else datetime.now().date()
    max_date_available = df_pedidos_detalhes[
        'data_pedido'].max().date() if not df_pedidos_detalhes.empty else datetime.now().date()

    # Ajusta min/max para garantir que data_input não dê erro se o DF estiver vazio
    if min_date_available > max_date_available:  # Caso só tenha um dia de dados ou dados inválidos
        # Se os dados estão vazios ou com range inválido, define um range padrão razoável
        min_date_available = datetime.now().date() - timedelta(days=365 * 5)  # 5 anos para trás
        max_date_available = datetime.now().date()  # Hoje

    # --- NOVO AJUSTE: Garantir que default_start/end_date estejam dentro dos limites dos dados ---
    today = datetime.now().date()

    period_options = {
        "Últimos 7 dias": (today - timedelta(days=7), today),
        "Últimos 30 dias": (today - timedelta(days=30), today),
        "Mês Atual": (today.replace(day=1), today),
        "Ano Atual": (today.replace(month=1, day=1), today),
        "Todo o Período": (min_date_available, max_date_available)
    }

    selected_period_name = st.sidebar.selectbox("Selecione um Período Rápido", list(period_options.keys()), index=4)
    default_start_date_candidate, default_end_date_candidate = period_options[selected_period_name]

    # Ajusta as datas padrão para ficarem dentro do range min_value e max_value dos dados
    default_end_date = min(default_end_date_candidate, max_date_available)
    default_start_date = max(min(default_start_date_candidate, default_end_date), min_date_available)

    # Se, após o ajuste, a data de início ainda for maior que a de fim, define para o "Todo o Período"
    if default_start_date > default_end_date:
        default_start_date = min_date_available
        default_end_date = max_date_available

    start_date = st.sidebar.date_input("Data de Início", value=default_start_date, min_value=min_date_available,
                                       max_value=max_date_available)
    end_date = st.sidebar.date_input("Data de Fim", value=default_end_date, min_value=min_date_available,
                                     max_value=max_date_available)

    if start_date > end_date:
        st.sidebar.error("A data de início não pode ser posterior à data de fim.")
        # Ajusta para um período válido para não quebrar o dashboard
        start_date = end_date - timedelta(days=1) if end_date > min_date_available else min_date_available
        st.sidebar.info(f"Ajustando data de início para {start_date.strftime('%d/%m/%Y')}.")

    # Filtrar dataframes globais com base nos filtros da barra lateral, se aplicável
    df_pedidos_filtrado = df_pedidos_detalhes[
        (df_pedidos_detalhes['data_pedido'].dt.date >= start_date) &
        (df_pedidos_detalhes['data_pedido'].dt.date <= end_date)
        ]
    df_pagamentos_filtrado = df_pagamentos[
        (df_pagamentos['data_pagamento'].dt.date >= start_date) &
        (df_pagamentos['data_pagamento'].dt.date <= end_date)
        ]

    # O df_estoque e df_produtos não são filtrados por data diretamente em seus KPIs principais
    # mas podem ser filtrados em seções específicas se necessário.

    # --- Exibição das Seções do Dashboard ---
    display_kpis(df_pedidos_filtrado, df_clientes)  # KPIs agora usam dados filtrados
    display_sales_trends(df_pedidos_detalhes, start_date, end_date)  # Gráficos de tendência usam filtros de data
    display_product_analysis(df_pedidos_filtrado, df_produtos)  # Análise de produtos também usa dados filtrados
    display_client_analysis(df_clientes, df_pedidos_filtrado)  # Análise de clientes também usa dados filtrados
    display_order_status(df_pedidos_filtrado)  # Status de pedidos usa dados filtrados
    display_payment_analysis(df_pagamentos_filtrado)  # Análise de pagamentos usa dados filtrados
    display_stock_analysis(df_estoque, df_produtos)  # Análise de estoque não é diretamente por data de pedido/pagamento
    display_products_by_cut_type(df_produtos)  # Análise de produtos por tipo de corte é estática por produto

    st.markdown("---")
    st.write("Dados atualizados automaticamente. Última atualização: " + datetime.now().strftime("%H:%M:%S"))