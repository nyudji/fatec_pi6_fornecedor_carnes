import psycopg2
import random
from datetime import datetime, timedelta
from faker import Faker
from psycopg2.extras import execute_values
import unicodedata
import time

# --- CONFIGURAÇÃO DE CONEXÃO ---
conn = psycopg2.connect(
    dbname="carnes",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)
conn.autocommit = False
cursor = conn.cursor()
fake = Faker("pt_BR")
regioes_sp = ["São Paulo", "Guarulhos", "Osasco", "Barueri", "Santo André", "São Bernardo do Campo"]

BATCH_SIZE_CLIENTES = 1000
BATCH_SIZE_ENTRADAS = 2000
BATCH_SIZE_PRODUTO_ENTRADA = 5000
BATCH_SIZE_ESTOQUE = 5000
BATCH_SIZE_PEDIDOS = 5000
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
conn.commit()
print(f"Fornecedor fixo inserido com ID: {id_fornecedor_fixo}")

# --- INSERIR PRODUTOS FIXOS ---
print("Inserindo produtos fixos...")
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
    ("Chorizo fatiado", "Chorizo", 119.00)
]
produto_data_list = []
for nome, tipo, preco_venda in produtos_para_inserir:
    preco_compra = round(preco_venda * random.uniform(0.7, 0.9), 2)
    produto_data_list.append((nome, tipo, "Kg", preco_compra, preco_venda, id_fornecedor_fixo))

execute_values(cursor, """
    INSERT INTO tb_produto (nome_produto, tipo_corte, unidade_medida, preco_compra, preco_venda, id_fornecedor)
    VALUES %s
""", produto_data_list)
conn.commit()
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

num_clientes = 500
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
    conn.commit()
    print(f"  {len(batch)} clientes inseridos. Total: {i + len(batch)}/{num_clientes}")
print(f"Total de {num_clientes} clientes inseridos.")

# --- ENTRADAS E PRODUTO_ENTRADA ---
print("Gerando e inserindo entradas e produtos de entrada...")
cursor.execute("SELECT id_produto, preco_compra FROM tb_produto")
produtos_info = cursor.fetchall()

num_entradas = 5000
entradas_data_list = []
for _ in range(num_entradas):
    data = datetime.now() - timedelta(days=random.randint(1, 90), hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
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
    conn.commit()
    print(f"  {len(batch)} entradas inseridas. Total: {i + len(batch)}/{num_entradas}")

produtos_entrada_data_list = []
itens_para_estoque_temp = []
for entrada_id, data_entrada in inserted_entrada_ids:
    for _ in range(random.randint(1, 4)):
        prod = random.choice(produtos_info)
        qtd = random.randint(50, 300)
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
    conn.commit()
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
    conn.commit()
    print(f"  {len(batch)} itens de estoque inseridos. Total: {i + len(batch)}/{len(estoque_data_list)}")
print(f"Total de {len(estoque_data_list)} itens de estoque inseridos.")


# --- PEDIDOS E ITENS DO PEDIDO ---
print("Gerando e inserindo pedidos e seus itens (com valor_total calculado em Python)...")
num_pedidos = 50000
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
    temp_produtos_info = produtos_info

for i in range(0, num_pedidos, BATCH_SIZE_PEDIDOS):
    pedidos_batch_data = []
    itens_pedido_batch_data = []
    
    current_batch_pedidos_simulados = []

    for _ in range(min(BATCH_SIZE_PEDIDOS, num_pedidos - i)):
        data_pedido = datetime.now() - timedelta(days=random.randint(1, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        status = random.choices(
            ["Pendente", "Faturado", "Entregue", "Cancelado"],
            weights=[0.3, 0.3, 0.35, 0.05],
            k=1
        )[0]
        
        # Corrigido: Inicializar current_pedido_total como float
        current_pedido_total = 0.0
        current_pedido_itens = []
        num_itens_por_pedido = random.randint(2, 5)
        
        for _ in range(num_itens_por_pedido):
            if estoque_disp:
                est = random.choice(estoque_disp)
                qtd = random.randint(1, min(est[1], 20))
                preco_unitario = float(est[4]) # Garantir que é float
                produto_id = est[2]
                unidade_medida = est[3]
            else:
                random_prod = random.choice(temp_produtos_info)
                qtd = random.randint(1, 10)
                preco_unitario = float(random_prod[1]) # Garantir que é float
                produto_id = random_prod[0]
                unidade_medida = "Kg"
            
            # Corrigido: item_total como float
            item_total = float(round(qtd * preco_unitario, 2))
            current_pedido_total += item_total
            current_pedido_itens.append((produto_id, qtd, unidade_medida, preco_unitario))

        pedidos_batch_data.append((random.randint(1, num_clientes), data_pedido, status, round(current_pedido_total, 2)))
        current_batch_pedidos_simulados.append((pedidos_batch_data[-1], current_pedido_itens))

    execute_values(cursor, """
        INSERT INTO tb_pedido (id_cliente, data_pedido, status, valor_total)
        VALUES %s
        RETURNING id_pedido
    """, pedidos_batch_data)
    current_batch_pedido_ids = [r[0] for r in cursor.fetchall()]
    all_inserted_pedido_ids.extend(current_batch_pedido_ids)
    conn.commit()
    print(f"  {len(pedidos_batch_data)} pedidos inseridos. Total: {i + len(pedidos_batch_data)}/{num_pedidos}")

    for idx, pid in enumerate(current_batch_pedido_ids):
        _, simulated_itens = current_batch_pedidos_simulados[idx]
        for item in simulated_itens:
            itens_pedido_batch_data.append((pid, item[0], item[1], item[2], item[3]))
    
    if itens_pedido_batch_data:
        execute_values(cursor, """
            INSERT INTO tb_item_pedido (id_pedido, id_produto, quantidade, unidade_medida, preco_unitario)
            VALUES %s
        """, itens_pedido_batch_data)
        conn.commit()
        print(f"    {len(itens_pedido_batch_data)} itens de pedido inseridos para o lote atual.")

print(f"Total de {len(all_inserted_pedido_ids)} pedidos inseridos.")
print("Valor_total dos pedidos agora está preenchido durante a inserção.")


# --- PAGAMENTOS ---
print("Gerando e inserindo pagamentos...")
cursor.execute("SELECT id_pedido, data_pedido FROM tb_pedido WHERE status IN ('Faturado', 'Entregue')")
pagaveis = cursor.fetchall()

pagamentos_data_list = []
metodos = ["PIX", "Cartão de crédito", "Cartão de débito", "Boleto bancário", "Transferência bancária (TED)"]
for i, (pid, data) in enumerate(pagaveis):
    status = random.choices(["Pago", "Aguardando pagamento", "Cancelado"], weights=[0.8, 0.15, 0.05])[0]
    data_pagamento_com_hora = data + timedelta(days=random.randint(1, 5), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    pagamentos_data_list.append((pid, data_pagamento_com_hora, f"LS2025{i+1:05d}", round(random.uniform(100, 5000), 2), random.choice(metodos), status))

for i in range(0, len(pagamentos_data_list), BATCH_SIZE_PAGAMENTOS):
    batch = pagamentos_data_list[i:i + BATCH_SIZE_PAGAMENTOS]
    execute_values(cursor, """
        INSERT INTO tb_pagamento (id_pedido, data_pagamento, lote_saida, valor_pago, metodo_pagamento, status)
        VALUES %s
    """, batch)
    conn.commit()
    print(f"  {len(batch)} pagamentos inseridos.")
print(f"Total de {len(pagamentos_data_list)} pagamentos inseridos.")

# --- FINALIZAÇÃO ---
cursor.close()
conn.close()
end_time = time.time()
print(f"\nPopulação completa de registros realizada com sucesso em {end_time - start_time:.2f} segundos.")