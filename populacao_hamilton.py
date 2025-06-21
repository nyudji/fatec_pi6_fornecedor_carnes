# popular_expansao_400K.py
import random
from datetime import datetime, timedelta
from faker import Faker
from psycopg2.extras import execute_values
import unicodedata
import time

# --- IMPORTAÇÃO DA SUA CLASSE DE CONEXÃO ---
from driver.psycopg2_connect import PostgresConnect

# --- CONFIGURAÇÃO DE CONEXÃO ---
db_connection = PostgresConnect(autocommit=False)
if db_connection.conn is None or db_connection.conn.closed:
    print("Erro: Não foi possível estabelecer a conexão com o banco de dados. Saindo do script.")
    exit()

cursor = db_connection.get_cursor()
if cursor is None:
    print("Erro: Não foi possível obter um cursor para o banco de dados. Saindo do script.")
    db_connection.close_connection()
    exit()

fake = Faker("pt_BR")
regioes_sp = ["São Paulo", "Guarulhos", "Osasco", "Barueri", "Santo André", "São Bernardo do Campo"]

# --- CONFIGURAÇÃO DE BATCH SIZES ---
BATCH_SIZE_CLIENTES = 500
BATCH_SIZE_ENTRADAS = 500
BATCH_SIZE_PRODUTO_ENTRADA = 1000
BATCH_SIZE_ESTOQUE = 1000
BATCH_SIZE_PEDIDOS = 250
BATCH_SIZE_PAGAMENTOS = 1000

# --- MÉTRICAS REALISTAS ---
NUM_CLIENTES = 500
NUM_ENTRADAS = 3000
NUM_PEDIDOS = 2200
ITENS_MIN = 8
ITENS_MAX = 20

print("Iniciando população do banco de dados...")
start_time = time.time()
# --- INSERIR FORNECEDOR FIXO ---
print("Inserindo fornecedor fixo...")
cursor.execute("""
    INSERT INTO tb_fornecedor (nome_fornecedor, cnpj_fornecedor, telefone_fornecedor, email_fornecedor, endereco_fornecedor)
    VALUES (%s, %s, %s, %s, %s)
    RETURNING id_fornecedor;
""", (
    fake.company(),
    fake.cnpj(),
    f"(11) 9{random.randint(4000,9999)}-{random.randint(1000,9999)}",
    fake.company_email(),
    f"{fake.street_name()}, {fake.building_number()} - {random.choice(regioes_sp)}"
))
id_fornecedor_fixo = cursor.fetchone()[0]
db_connection.commit()
print(f"Fornecedor fixo inserido com ID: {id_fornecedor_fixo}")
# --- INSERIR TODOS OS PRODUTOS FIXOS E ADICIONAIS ---
print("Inserindo todos os produtos fixos e adicionais...")
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
db_connection.commit()
print(f"{len(produto_data_list)} produtos fixos e adicionais inseridos.")
# --- INSERIR CLIENTES ---
def gerar_email(nome):
    dominios = ["gmail.com", "hotmail.com", "outlook.com"]
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('utf-8')
    partes = nome.lower().split()
    nome_formatado = ".".join(partes)
    while ".." in nome_formatado:
        nome_formatado = nome_formatado.replace("..", ".")
    return f"{nome_formatado}{random.randint(1,999)}@{random.choice(dominios)}"

print("Inserindo clientes...")
clientes = []
for _ in range(NUM_CLIENTES):
    nome = fake.name()
    clientes.append((
        nome,
        fake.cnpj(),
        f"(11) 9{random.randint(4000,9999)}-{random.randint(1000,9999)}",
        gerar_email(nome),
        f"{fake.street_name()}, {fake.building_number()} - {random.choice(regioes_sp)}",
        random.choice(["Restaurante", "Mini mercado", "Mercearia"])
    ))

execute_values(cursor, """
    INSERT INTO tb_cliente (nome_cliente, cnpj_cliente, telefone_cliente, email_cliente, endereco_cliente, tipo_cliente)
    VALUES %s
""", clientes)
db_connection.commit()

# --- INSERIR ENTRADAS ---
print("Gerando entradas de produtos...")
entradas = [(datetime.now() - timedelta(days=random.randint(1, 90)), id_fornecedor_fixo) for _ in range(NUM_ENTRADAS)]

execute_values(cursor, """
    INSERT INTO tb_entrada (data_entrada, id_fornecedor)
    VALUES %s
    RETURNING id_entrada, data_entrada
""", entradas)

entrada_ids = cursor.fetchall()
db_connection.commit()
# --- INSERIR PRODUTO_ENTRADA + ESTOQUE ---
produto_entrada = []
estoque = []
estoque_disponivel = {}

cursor.execute("SELECT id_produto, preco_compra, preco_venda, unidade_medida FROM tb_produto")
produtos = cursor.fetchall()

for entrada_id, data in entrada_ids:
    for _ in range(random.randint(1, 3)):
        prod_id, preco_compra, preco_venda, unidade = random.choice(produtos)
        qtd = random.randint(30, 150)
        total = round(qtd * preco_compra, 2)
        validade = data + timedelta(days=random.randint(15, 60))
        lote = f"L{entrada_id}P{prod_id}"
        produto_entrada.append((entrada_id, prod_id, qtd, total, validade, lote))

execute_values(cursor, """
    INSERT INTO tb_produto_entrada (id_entrada, id_produto, quantidade, preco_total, validade, lote)
    VALUES %s
    RETURNING id_item_entrada, id_produto, quantidade
""", produto_entrada)

entradas_result = cursor.fetchall()
db_connection.commit()

for item_id, prod_id, qtd in entradas_result:
    qtd_disp = random.randint(qtd // 2, qtd)
    estoque.append((item_id, qtd_disp, random.choice(["Freezer", "Prateleira A1", "Despacho"])))
    estoque_disponivel[item_id] = {"id_produto": prod_id, "quantidade": qtd_disp}

execute_values(cursor, """
    INSERT INTO tb_estoque (item_entrada, quantidade_disponivel, localizacao)
    VALUES %s
""", estoque)
db_connection.commit()
# --- INSERIR PEDIDOS E ITENS ---
print("Gerando pedidos...")
cursor.execute("SELECT id_cliente FROM tb_cliente")
clientes_ids = [row[0] for row in cursor.fetchall()]

pedido_data = []
item_data = []
estoque_ids = list(estoque_disponivel.keys())
random.shuffle(estoque_ids)

for _ in range(NUM_PEDIDOS):
    if not estoque_ids:
        break
    cliente_id = random.choice(clientes_ids)
    data = datetime.now() - timedelta(days=random.randint(1, 40))
    status = random.choice(["Faturado", "Entregue"])
    total = 0
    itens = []
    for _ in range(random.randint(ITENS_MIN, ITENS_MAX)):
        if not estoque_ids:
            break
        item_id = random.choice(estoque_ids)
        if estoque_disponivel[item_id]['quantidade'] <= 0:
            estoque_ids.remove(item_id)
            continue
        qtd = random.randint(1, min(20, estoque_disponivel[item_id]['quantidade']))
        estoque_disponivel[item_id]['quantidade'] -= qtd
        if estoque_disponivel[item_id]['quantidade'] <= 0:
            estoque_ids.remove(item_id)
        prod_id = estoque_disponivel[item_id]['id_produto']
        cursor.execute("SELECT preco_venda, unidade_medida FROM tb_produto WHERE id_produto = %s", (prod_id,))
        preco, unidade = cursor.fetchone()
        total += round(preco * qtd, 2)
        itens.append((prod_id, qtd, unidade, preco))
    if not itens:
        continue
    pedido_data.append((cliente_id, data, status, total))
    item_data.append(itens)

execute_values(cursor, """
    INSERT INTO tb_pedido (id_cliente, data_pedido, status, valor_total)
    VALUES %s
    RETURNING id_pedido
""", pedido_data)

pedido_ids = cursor.fetchall()
db_connection.commit()

final_items = []
for i, (pid,) in enumerate(pedido_ids):
    for prod_id, qtd, unidade, preco in item_data[i]:
        final_items.append((pid, prod_id, qtd, unidade, preco))

execute_values(cursor, """
    INSERT INTO tb_item_pedido (id_pedido, id_produto, quantidade, unidade_medida, preco_unitario)
    VALUES %s
""", final_items)
db_connection.commit()

print(f"Total de pedidos inseridos: {len(pedido_ids)}")
print(f"Total de itens inseridos: {len(final_items)}")

# --- INSERIR PAGAMENTOS ---
pagamentos = []
for i, (pid,) in enumerate(pedido_ids):
    metodo = random.choice(["PIX", "Cartão de crédito", "Boleto bancário"])
    status = random.choices(["Pago", "Aguardando pagamento", "Cancelado"], weights=[0.85, 0.10, 0.05])[0]
    dt = datetime.now() - timedelta(days=random.randint(1, 10))
    ref = f"REF{datetime.now().strftime('%Y%m%d%H%M%S')}{i:05d}"
    valor = round(random.uniform(200, 2500), 2)
    pagamentos.append((pid, dt, ref, valor, metodo, status))

execute_values(cursor, """
    INSERT INTO tb_pagamento (id_pedido, data_pagamento, lote_saida, valor_pago, metodo_pagamento, status)
    VALUES %s
""", pagamentos)
db_connection.commit()

# --- FINALIZAÇÃO ---
cursor.close()
db_connection.close_connection()
end_time = time.time()
print(f"\\nPopulação completa de registros realizada com sucesso em {end_time - start_time:.2f} segundos.")
