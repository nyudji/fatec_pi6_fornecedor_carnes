import random
from datetime import datetime, timedelta
from faker import Faker
from psycopg2.extras import execute_values
import unicodedata
import time

# --- IMPORTAÇÃO DA SUA CLASSE DE CONEXÃO ---
from driver.psycopg2_connect import PostgresConnect 

# --- CONFIGURAÇÃO DE CONEXÃO ---
# Agora, instanciamos sua classe PostgresConnect
# Passamos autocommit=False para gerenciar transações manualmente, como você já fazia.
db_connection = PostgresConnect(autocommit=False) 

# Verifica se a conexão foi bem-sucedida antes de prosseguir
if db_connection.conn is None or db_connection.conn.closed:
    print("Erro: Não foi possível estabelecer a conexão com o banco de dados. Saindo do script.")
    exit() # Sai do script se a conexão falhou

cursor = db_connection.get_cursor() # Obtém o cursor através do método da classe

# Verifica se o cursor foi obtido com sucesso
if cursor is None:
    print("Erro: Não foi possível obter um cursor para o banco de dados. Saindo do script.")
    db_connection.close_connection()
    exit()

fake = Faker("pt_BR")
regioes_sp = ["São Paulo", "Guarulhos", "Osasco", "Barueri", "Santo André", "São Bernardo do Campo"]

BATCH_SIZE_CLIENTES = 1000
BATCH_SIZE_ENTRADAS = 2000
BATCH_SIZE_PRODUTO_ENTRADA = 5000
BATCH_SIZE_ESTOQUE = 5000
BATCH_SIZE_PEDIDOS = 500 # Mantido do original
BATCH_SIZE_ITENS_PEDIDO = 10000 
BATCH_SIZE_PAGAMENTOS = 10000

print("Iniciando população do banco de dados...")
start_time = time.time()

# --- INSERIR FORNECEDOR FIXO ---
print("Inserindo fornecedor fixo...")
cursor.execute("""
    INSERT INTO tb_fornecedor (nome_fornecedor, cnpj_fornecedor, telefone_fornecedor, email_fornecedor, endereco_fornecedor)
    VALUES (%s, %s, %s, %s, %s)
    RETURNING id_fornecedor;
""", (
    fake.company(), fake.cnpj(), f"(11) 9{random.randint(4000,9999)}-{random.randint(1000,9999)}", fake.company_email(),
    f"{fake.street_name()}, {fake.building_number()} - {random.choice(regioes_sp)}"
))
id_fornecedor_fixo = cursor.fetchone()[0]
db_connection.commit() # Usa o método de commit da sua classe
print(f"Fornecedor fixo inserido com ID: {id_fornecedor_fixo}")

# --- INSERIR PRODUTOS FIXOS ---
print("Inserindo produtos fixos...")
# Nova lista de produtos (já adicionada na versão anterior)
produtos_para_inserir = [
    ("Tulipa de frango temperada com shoyu", "Tulipa", 40.00),
    ("Lombo fatiado", "Lombo", 47.00),
    ("Barriga", "Barriga", 53.00),
    ("Lagarto fatiado", "Lagarto", 65.00),
    ("Patinho fatiado", "Patinho", 65.00),
    ("Coxão mole", "Coxão mole", 65.00),
    ("Miolo de alcatra", "Alcatra", 85.00),
    ("Contra filé", "Contra filé", 89.00),
    ("Cupim", "Cupim", 89.00),
    ("Filé mignon", "Filé mignon", 129.00),
    ("Cordeiro temperado", "Cordeiro", 79.00),
    ("Carré", "Carré", 99.00),
    ("Ancho fatiado", "Ancho", 124.00),
    ("Chorizo fatiado", "Chorizo", 119.00),
    ("Tulipa de frango temperada com gengibre", "Tulipa", 40.00),
    ("Tulipa de frango temperada com missô", "Tulipa", 40.00),
    ("Tulipa de frango temperada com kimchi", "Tulipa", 40.00),
    ("Copa lombo fatiado", "Copa lombo", 47.00),
    ("Filé suíno", "Filé suíno", 47.00),
    ("Lombo (peça)", "Lombo", 42.00),
    ("Bisteca fatiada", "Bisteca", 40.00),
    ("Copa lombo (peça)", "Copa lombo", 42.00),
    ("Filé suíno sem tempero", "Filé suíno", 42.00),
    ("Filé suíno temperado", "Filé suíno", 43.00),
    ("Costela em ripas sem couro", "Costela", 38.00),
    ("Costela temperada com alho, limão e pimenta", "Costela", 40.00),
    ("Costela temperada com missô", "Costela", 40.00),
    ("Panceta com alho e pimenta", "Panceta", 49.00),
    ("Panceta com kimchi (1kg)", "Panceta", 49.00),
    ("Panceta com kimchi (500g)", "Panceta", 25.00),
    ("Panceta com missô (1kg)", "Panceta", 49.00),
    ("Panceta com missô (500g)", "Panceta", 25.00),
    ("Joelho suíno", "Joelho", 39.00),
    ("Linguiça alho tradicional (1kg)", "Linguiça", 42.00),
    ("Linguiça alho light (1kg)", "Linguiça", 42.00),
    ("Linguiça missô (1kg)", "Linguiça", 42.00),
    ("Linguiça gengibre (1kg)", "Linguiça", 42.00),
    ("Linguiça nirá (1kg)", "Linguiça", 42.00),
    ("Linguiça curry (1kg)", "Linguiça", 42.00),
    ("Linguiça wasabi (1kg)", "Linguiça", 42.00),
    ("Linguiça muçarela (1kg)", "Linguiça", 42.00),
    ("Linguiça codeguim (1kg)", "Linguiça", 42.00),
    ("Linguiça sortida (500g)", "Linguiça", 23.00),
    ("Manta de pernil temperada (1kg)", "Manta de pernil", 42.00),
    ("Manta de pernil temperada (500g)", "Manta de pernil", 23.00),
    ("Salame (peça 400g)", "Salame", 42.00),
    ("Salame fatiado (200g)", "Salame", 24.00),
    ("Banha de porco (450g)", "Banha", 27.00),
    ("Pé de porco", "Pé", 18.00),
    ("Pele de porco (courinho)", "Pele", 12.00),
    ("Picanha de novilho (acima 800g)", "Picanha", 129.00),
    ("Picanha de novilho (até 800g)", "Picanha", 139.00),
    ("Filé mignon de novilho", "Filé mignon", 124.00),
    ("Contra filé de novilho", "Contra filé", 85.00),
    ("Miolo da alcatra de novilho", "Alcatra", 82.00),
    ("Miolo da alcatra de novilho no sereno", "Alcatra", 82.00),
    ("Miolo da paleta de novilho", "Paleta", 55.00),
    ("Maminha de novilho", "Maminha", 82.00),
    ("Fraldinha de novilho sem tempero", "Fraldinha", 74.00),
    ("Fraldinha de novilho temperada", "Fraldinha", 76.00),
    ("Pacú da fraldinha de novilho", "Fraldinha", 74.00),
    ("Chuleta de novilho", "Chuleta", 70.00),
    ("Coxão mole de novilho", "Coxão mole", 55.00),
    ("Patinho de novilho", "Patinho", 55.00),
    ("Lagarto (peça)", "Lagarto", 55.00),
    ("Peixinho (moído ou peça)", "Peixinho", 50.00),
    ("Cupim de novilho sem tempero", "Cupim", 82.00),
    ("Cupim de novilho com missô", "Cupim", 84.00),
    ("Cupim de novilho com kimchi", "Cupim", 84.00),
    ("Cupim de novilho fatiado sem tempero", "Cupim", 83.00),
    ("Cupim de novilho temperado para churrasco", "Cupim", 84.00),
    ("Costelão de novilho", "Costela", 49.00),
    ("Costela de tiras novilho", "Costela", 49.00),
    ("Kafta mista (1kg)", "Kafta", 54.00),
    ("Músculo traseiro de novilho", "Músculo", 49.00),
    ("Acém de novilho", "Acém", 49.00),
    ("Rabo de novilho", "Rabo", 49.00),
    ("Picanha black angus", "Picanha", 184.00),
    ("Short rib black angus", "Short rib", 99.00),
    ("Chorizo black angus", "Chorizo", 124.00),
    ("Ancho black angus", "Ancho", 129.00),
    ("T-bone black angus", "T-bone", 119.00),
    ("Prime rib black angus", "Prime rib", 119.00),
    ("Tomahawk", "Tomahawk", 119.00),
    ("Denver steak", "Denver", 169.00),
    ("Baby beef", "Baby beef", 95.00),
    ("Shoulder steak", "Shoulder", 99.00),
    ("Fraldinha black angus", "Fraldinha", 94.00),
    ("Pacú da fraldinha black angus", "Fraldinha", 94.00),
    ("Entranha black angus", "Entranha", 94.00),
    ("Maminha black angus", "Maminha", 94.00),
    ("Lagarto black angus", "Lagarto", 60.00),
    ("Peixinho black angus", "Peixinho", 55.00),
    ("Brisket black angus", "Brisket", 55.00),
    ("Costela de tiras black angus", "Costela", 55.00),
    ("Rabo black angus", "Rabo", 49.00),
    ("Linguiça bovina angus (500g)", "Linguiça", 24.00),
    ("Linguiça bovina angus (1kg)", "Linguiça", 47.00),
    ("Manta bovina angus (500g)", "Manta bovina", 24.00),
    ("Manta bovina angus (1kg)", "Manta bovina", 47.00),
    ("Linguiça cuiabana angus (1kg)", "Linguiça cuiabana", 49.00),
    ("Linguiça cuiabana angus (500g)", "Linguiça cuiabana", 25.00),
    ("Linguiça mista angus + suína (1kg)", "Linguiça mista", 45.00),
    ("Linguiça mista angus + suína (500g)", "Linguiça mista", 23.00)
]

produto_data_list = []
for nome, tipo, preco_venda in produtos_para_inserir:
    preco_compra = round(preco_venda * random.uniform(0.7, 0.9), 2)
    produto_data_list.append((nome, tipo, "Kg", preco_compra, preco_venda, id_fornecedor_fixo))

execute_values(cursor, """
    INSERT INTO tb_produto (nome_produto, tipo_corte, unidade_medida, preco_compra, preco_venda, id_fornecedor)
    VALUES %s
""", produto_data_list)
db_connection.commit() # Usa o método de commit da sua classe
print(f"{len(produto_data_list)} produtos fixos inseridos.")

# --- INSERIR CLIENTES ---
print("Gerando e inserindo clientes...")
def gerar_email(nome):
    dominios = ["gmail.com", "hotmail.com", "outlook.com"]
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('utf-8')
    prefixos = ["sr.", "sra.", "dr.", "dra.", "sr", "sra", "dr", "dra"]
    partes = nome.lower().split()
    partes_filtradas = [p for p in partes if p not in prefixos]
    nome_formatado = ".".join(partes_filtradas)
    while ".." in nome_formatado:
        nome_formatado = nome_formatado.replace("..", ".")
    return f"{nome_formatado}{random.randint(1, 999)}@{random.choice(dominios)}"

num_clientes = 420 # Mantido em 420 clientes
clientes_gerados = []
for _ in range(num_clientes):
    nome = fake.name()
    clientes_gerados.append((
        nome,
        fake.cnpj(),
        f"(11) 9{random.randint(4000,9999)}-{random.randint(1000,9999)}",
        gerar_email(nome),
        f"{fake.street_name()}, {fake.building_number()} - {random.choice(regioes_sp)}",
        random.choice(["Atacado", "Varejo", "Restaurante", "Mercado"])
    ))

for i in range(0, len(clientes_gerados), BATCH_SIZE_CLIENTES):
    batch = clientes_gerados[i:i + BATCH_SIZE_CLIENTES]
    execute_values(cursor, """
        INSERT INTO tb_cliente (nome_cliente, cnpj_cliente, telefone_cliente, email_cliente, endereco_cliente, tipo_cliente)
        VALUES %s
    """, batch)
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"  {len(batch)} clientes inseridos. Total: {i + len(batch)}/{num_clientes}")
print(f"Total de {num_clientes} clientes inseridos.")

# --- ENTRADAS E PRODUTO_ENTRADA ---
print("Gerando e inserindo entradas e produtos de entrada...")
cursor.execute("SELECT id_produto, preco_compra FROM tb_produto")
produtos_info = cursor.fetchall()

num_entradas = 5000 
entradas_data_list = []
for _ in range(num_entradas):
    data = datetime.now() - timedelta(days=random.randint(1, 365), hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    entradas_data_list.append((data, id_fornecedor_fixo))

inserted_entrada_ids = []
for i in range(0, len(entradas_data_list), BATCH_SIZE_ENTRADAS):
    batch = entradas_data_list[i:i + BATCH_SIZE_ENTRADAS]
    execute_values(cursor, """
        INSERT INTO tb_entrada (data_entrada, id_fornecedor)
        VALUES %s
        RETURNING id_entrada, data_entrada
    """, batch)
    inserted_entrada_ids.extend(cursor.fetchall())
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"  {len(batch)} entradas inseridas. Total: {i + len(batch)}/{num_entradas}")

produtos_entrada_data_list = []
itens_para_estoque_temp = []
for entrada_id, data_entrada in inserted_entrada_ids:
    for _ in range(random.randint(5, 10)): 
        prod = random.choice(produtos_info)
        qtd = random.randint(30, 100) 
        total = round(qtd * prod[1], 2)
        validade = data_entrada + timedelta(days=random.randint(15, 60))
        lote = f"L{data_entrada.strftime('%Y%m%d')}FR{prod[0]:03d}"
        produtos_entrada_data_list.append((entrada_id, prod[0], qtd, total, validade, lote))

for i in range(0, len(produtos_entrada_data_list), BATCH_SIZE_PRODUTO_ENTRADA):
    batch = produtos_entrada_data_list[i:i + BATCH_SIZE_PRODUTO_ENTRADA]
    execute_values(cursor, """
        INSERT INTO tb_produto_entrada (id_entrada, id_produto, quantidade, preco_total, validade, lote)
        VALUES %s
        RETURNING id_item_entrada, quantidade
    """, batch)
    itens_para_estoque_temp.extend(cursor.fetchall())
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"  {len(batch)} produtos de entrada inseridos.")
print(f"Total de {len(produtos_entrada_data_list)} produtos de entrada inseridos.")

# --- ESTOQUE ---
print("Gerando e inserindo estoque...")
estoque_data_list = [
    (item[0], random.randint(item[1] // 2, item[1]), random.choice(["Câmara Fria 1", "Freezer", "Prateleira A1", "Despacho"]))
    for item in itens_para_estoque_temp
]

for i in range(0, len(estoque_data_list), BATCH_SIZE_ESTOQUE):
    batch = estoque_data_list[i:i + BATCH_SIZE_ESTOQUE]
    execute_values(cursor, """
        INSERT INTO tb_estoque (item_entrada, quantidade_disponivel, localizacao)
        VALUES %s
    """, batch)
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"  {len(batch)} itens de estoque inseridos. Total: {i + len(batch)}/{len(estoque_data_list)}")
print(f"Total de {len(estoque_data_list)} itens de estoque inseridos.")


# --- PEDIDOS E ITENS DO PEDIDO ---
print("Gerando e inserindo pedidos e seus itens (com valor_total calculado em Python)...")
num_dias_simulacao = 1825 # 1 ano de dados
max_pedidos_por_dia = 10 # Mantido como máximo

all_inserted_pedido_ids = []

cursor.execute("""
    SELECT e.id_estoque, e.quantidade_disponivel, p.id_produto, p.unidade_medida, p.preco_venda
    FROM tb_estoque e
    JOIN tb_produto_entrada pe ON pe.id_item_entrada = e.item_entrada
    JOIN tb_produto p ON p.id_produto = pe.id_produto
    WHERE e.quantidade_disponivel > 0
""")
estoque_disp = cursor.fetchall()

if not estoque_disp:
    print("Aviso: Nenhum item disponível no estoque para criar itens de pedido. Usando produtos aleatórios para simulação.")
    temp_produtos_info = [(p[0], p[1], p[2]) for p in produtos_para_inserir] # Recria temp_produtos_info com id, nome, preco_venda
else:
    # Ajuste para garantir que temp_produtos_info esteja definida e contenha o id_produto, preco_venda.
    # Se estoque_disp não estiver vazio, podemos usar a mesma estrutura para temp_produtos_info, mas a partir dos produtos_info
    cursor.execute("SELECT id_produto, nome_produto, preco_venda FROM tb_produto")
    temp_produtos_info = cursor.fetchall()


num_total_itens_gerados = 0
total_faturamento_gerado = 0.0

# Loop por dias para simular o limite de pedidos diários
for day_offset in range(num_dias_simulacao):
    # Ajustado para gerar entre 5 e 10 pedidos por dia, para atingir a média de 200/mês (aprox. 6.5/dia)
    daily_pedidos_count = random.randint(5, max_pedidos_por_dia) 
    
    pedidos_batch_data = []
    itens_pedido_batch_data = []
    current_batch_pedidos_simulados = []

    for _ in range(daily_pedidos_count):
        data_pedido = datetime.now() - timedelta(days=day_offset, hours=random.randint(8, 18), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        status = random.choices(
            ["Pendente", "Faturado", "Entregue", "Cancelado"],
            weights=[0.3, 0.3, 0.35, 0.05],
            k=1
        )[0]
        
        current_pedido_total = 0.0
        current_pedido_itens = []
        
        # Ajuste: Número de itens por pedido para atingir ticket médio de R$1.000 a R$8.000
        # Média de preço de venda dos produtos adicionados (aproximadamente R$ 68.23)
        # Para R$1.000 -> ~15 itens; para R$8.000 -> ~117 itens.
        # Ajustando a faixa para 10 a 120 itens por pedido.
        num_itens_por_pedido = random.randint(2, 14) 
        
        for _ in range(num_itens_por_pedido):
            if estoque_disp:
                est = random.choice(estoque_disp)
                # Quantidade por item no pedido: reduzido para 1 a 3 unidades
                qtd = random.randint(1, min(est[1], 3)) 
                preco_unitario = float(est[4])
                produto_id = est[2]
                unidade_medida = est[3]
            else: # Fallback se estoque_disp estiver vazio
                random_prod = random.choice(temp_produtos_info)
                qtd = random.randint(1, 3) # Mesmo ajuste para fallback
                preco_unitario = float(random_prod[2]) # Usar preco_venda do temp_produtos_info
                produto_id = random_prod[0] # id_produto
                unidade_medida = "Kg" # Default
            
            item_total = float(round(qtd * preco_unitario, 2))
            current_pedido_total += item_total
            current_pedido_itens.append((produto_id, qtd, unidade_medida, preco_unitario))
            num_total_itens_gerados += 1

        pedidos_batch_data.append((random.randint(1, num_clientes), data_pedido, status, round(current_pedido_total, 2)))
        total_faturamento_gerado += round(current_pedido_total, 2)
        current_batch_pedidos_simulados.append((pedidos_batch_data[-1], current_pedido_itens))

    if pedidos_batch_data:
        execute_values(cursor, """
            INSERT INTO tb_pedido (id_cliente, data_pedido, status, valor_total)
            VALUES %s
            RETURNING id_pedido
        """, pedidos_batch_data)
        current_batch_pedido_ids = [r[0] for r in cursor.fetchall()]
        all_inserted_pedido_ids.extend(current_batch_pedido_ids)
        db_connection.commit() # Usa o método de commit da sua classe

        for idx, pid in enumerate(current_batch_pedido_ids):
            _, simulated_itens = current_batch_pedidos_simulados[idx]
            for item in simulated_itens:
                itens_pedido_batch_data.append((pid, item[0], item[1], item[2], item[3]))
        
        for i in range(0, len(itens_pedido_batch_data), BATCH_SIZE_ITENS_PEDIDO):
            batch_itens = itens_pedido_batch_data[i:i + BATCH_SIZE_ITENS_PEDIDO]
            if batch_itens:
                execute_values(cursor, """
                    INSERT INTO tb_item_pedido (id_pedido, id_produto, quantidade, unidade_medida, preco_unitario)
                    VALUES %s
                """, batch_itens)
                db_connection.commit() # Usa o método de commit da sua classe

print(f"Total de {len(all_inserted_pedido_ids)} pedidos inseridos em {num_dias_simulacao} dias.")
print(f"Total de {num_total_itens_gerados} itens de venda gerados em {num_dias_simulacao} dias.")
print(f"Total de faturamento gerado: R$ {total_faturamento_gerado:.2f}")
print(f"Média mensal de faturamento (baseada em {num_dias_simulacao} dias): R$ {total_faturamento_gerado / (num_dias_simulacao / 30):.2f}")
if len(all_inserted_pedido_ids) > 0:
    print(f"Ticket médio por pedido: R$ {total_faturamento_gerado / len(all_inserted_pedido_ids):.2f}")


# --- PAGAMENTOS ---
print("Gerando e inserindo pagamentos...")
cursor.execute("SELECT id_pedido, data_pedido, valor_total FROM tb_pedido WHERE status IN ('Faturado', 'Entregue')")
pagaveis = cursor.fetchall()

pagamentos_data_list = []
metodos = ["PIX", "Cartão de crédito", "Cartão de débito", "Boleto bancário", "Transferência bancária (TED)"]
for i, (pid, data, valor_total_pedido) in enumerate(pagaveis):
    status = random.choices(["Pago", "Aguardando pagamento", "Cancelado"], weights=[0.8, 0.15, 0.05])[0]
    data_pagamento_com_hora = data + timedelta(days=random.randint(1, 5), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    pagamentos_data_list.append((pid, data_pagamento_com_hora, f"LS2025{i+1:05d}", valor_total_pedido, random.choice(metodos), status))

for i in range(0, len(pagamentos_data_list), BATCH_SIZE_PAGAMENTOS):
    batch = pagamentos_data_list[i:i + BATCH_SIZE_PAGAMENTOS]
    execute_values(cursor, """
        INSERT INTO tb_pagamento (id_pedido, data_pagamento, lote_saida, valor_pago, metodo_pagamento, status)
        VALUES %s
    """, batch)
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"  {len(batch)} pagamentos inseridos.")
print(f"Total de {len(pagamentos_data_list)} pagamentos inseridos.")

# --- FINALIZAÇÃO ---
db_connection.close_connection() # Fecha a conexão e o cursor associado
end_time = time.time()
print(f"\nPopulação completa de registros realizada com sucesso em {end_time - start_time:.2f} segundos.")