import psycopg2
import random
from datetime import datetime, timedelta
from faker import Faker
from psycopg2.extras import execute_values
import unicodedata

# === CONFIGURAÇÃO DE CONEXÃO ===
conn = psycopg2.connect(
    dbname="carnes",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()
fake = Faker("pt_BR")
regioes_sp = ["São Paulo", "Guarulhos", "Osasco", "Barueri", "Santo André", "São Bernardo do Campo"]

# === INSERIR FORNECEDOR FIXO ===
cursor.execute("""
    INSERT INTO tb_fornecedor (nome_fornecedor, cnpj_fornecedor, telefone_fornecedor, email_fornecedor, endereco_fornecedor)
    VALUES (%s, %s, %s, %s, %s)
    RETURNING id_fornecedor; -- Adicionado RETURNING para obter o ID gerado
""", (
    fake.company(), fake.cnpj(), f"(11) 9{random.randint(4000,9999)}-{random.randint(1000,9999)}", fake.company_email(),
    f"{fake.street_name()}, {fake.building_number()} - {random.choice(regioes_sp)}"
))
id_fornecedor_fixo = cursor.fetchone()[0] # Recupera o ID do fornecedor recém-inserido

# === INSERIR PRODUTOS FIXOS ===
produtos = [
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
for nome, tipo, preco_venda in produtos:
    preco_compra = round(preco_venda * random.uniform(0.7, 0.9), 2)
    cursor.execute("""
        INSERT INTO tb_produto (nome_produto, tipo_corte, unidade_medida, preco_compra, preco_venda, id_fornecedor)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (nome, tipo, "Kg", preco_compra, preco_venda, id_fornecedor_fixo)) # Usa o ID recuperado aqui


# === INSERIR CLIENTES ===
def gerar_email(nome):
    dominios = ["gmail.com", "hotmail.com", "outlook.com"]
    
    # Remove acentos
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('utf-8')
    
    # Remove títulos como "Sr.", "Sra.", "Dr.", etc.
    prefixos = ["sr.", "sra.", "dr.", "dra.", "sr", "sra", "dr", "dra"]
    partes = nome.lower().split()
    partes_filtradas = [p for p in partes if p not in prefixos]

    # Junta nome e sobrenome
    nome_formatado = ".".join(partes_filtradas)

    # Remove pontos duplicados
    while ".." in nome_formatado:
        nome_formatado = nome_formatado.replace("..", ".")

    return f"{nome_formatado}{random.randint(1, 999)}@{random.choice(dominios)}"


clientes = [
    (
        nome := fake.name(),
        fake.cnpj(),
        f"(11) 9{random.randint(4000,9999)}-{random.randint(1000,9999)}",
        gerar_email(nome),
        f"{fake.street_name()}, {fake.building_number()} - {random.choice(regioes_sp)}",
        random.choice(["Atacado", "Varejo", "Restaurante", "Mercado"])
    )
    for _ in range(500)
]

# Inserir clientes no banco de dados
execute_values(cursor, """
    INSERT INTO tb_cliente (nome_cliente, cnpj_cliente, telefone_cliente, email_cliente, endereco_cliente, tipo_cliente)
    VALUES %s
""", clientes)

# === ENTRADAS E PRODUTO_ENTRADA ===
cursor.execute("SELECT id_produto, preco_compra FROM tb_produto")
produtos_info = cursor.fetchall()
entradas = []
produtos_entrada = []
for i in range(5000):
    # data_entrada agora inclui hora
    data = datetime.now() - timedelta(days=random.randint(1, 90), hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    entradas.append((data, id_fornecedor_fixo)) # Passa o objeto datetime completo e o ID do fornecedor fixo

execute_values(cursor, """
    INSERT INTO tb_entrada (data_entrada, id_fornecedor)
    VALUES %s
""", entradas)
cursor.execute("SELECT id_entrada, data_entrada FROM tb_entrada")
entradas_ids = cursor.fetchall()

for entrada_id, data_entrada in entradas_ids:
    for _ in range(random.randint(1, 4)):
        prod = random.choice(produtos_info)
        qtd = random.randint(50, 300)
        total = round(qtd * prod[1], 2)
        # validade agora considera a data completa
        validade = data_entrada + timedelta(days=random.randint(15, 60))
        lote = f"L{data_entrada.strftime('%Y%m%d')}FR{prod[0]:03d}"
        produtos_entrada.append((entrada_id, prod[0], qtd, total, validade, lote))

execute_values(cursor, """
    INSERT INTO tb_produto_entrada (id_entrada, id_produto, quantidade, preco_total, validade, lote)
    VALUES %s
""", produtos_entrada)

# === ESTOQUE ===
cursor.execute("SELECT id_item_entrada, quantidade FROM tb_produto_entrada")
itens = cursor.fetchall()
estoque = [
    (item[0], random.randint(item[1] // 2, item[1]), random.choice(["Câmara Fria 1", "Freezer", "Prateleira A1", "Despacho"]))
    for item in itens
]
execute_values(cursor, """
    INSERT INTO tb_estoque (item_entrada, quantidade_disponivel, localizacao)
    VALUES %s
""", estoque)

# === PEDIDOS ===
pedidos = []
for _ in range(50000):
    # data_pedido agora inclui hora
    data_pedido = datetime.now() - timedelta(days=random.randint(1, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    status = random.choices(
        ["Pendente", "Faturado", "Entregue", "Cancelado"],
        weights=[0.3, 0.3, 0.35, 0.05],  # Apenas 5% cancelados
        k=1
    )[0]
    pedidos.append((random.randint(1, 500), data_pedido, status, 0.0)) # Passa o objeto datetime completo
execute_values(cursor, """
    INSERT INTO tb_pedido (id_cliente, data_pedido, status, valor_total)
    VALUES %s
""", pedidos)
cursor.execute("SELECT id_pedido FROM tb_pedido")
ids_pedidos = [r[0] for r in cursor.fetchall()]

# === ITENS DO PEDIDO ===
cursor.execute("""
    SELECT e.id_estoque, e.quantidade_disponivel, p.id_produto, p.unidade_medida, p.preco_venda
    FROM tb_estoque e
    JOIN tb_produto_entrada pe ON pe.id_item_entrada = e.item_entrada
    JOIN tb_produto p ON p.id_produto = pe.id_produto
    WHERE e.quantidade_disponivel > 0
""")
estoque_disp = cursor.fetchall()

itens_pedido = []
for pid in ids_pedidos:
    for _ in range(random.randint(2, 5)):
        est = random.choice(estoque_disp)
        qtd = random.randint(1, min(est[1], 20))
        itens_pedido.append((pid, est[2], qtd, est[3], est[4]))
execute_values(cursor, """
    INSERT INTO tb_item_pedido (id_pedido, id_produto, quantidade, unidade_medida, preco_unitario)
    VALUES %s
""", itens_pedido)

# === ATUALIZA VALOR_TOTAL DOS PEDIDOS ===
cursor.execute("""
    UPDATE tb_pedido p SET valor_total = COALESCE((
        SELECT SUM(ip.quantidade * ip.preco_unitario)
        FROM tb_item_pedido ip
        WHERE ip.id_pedido = p.id_pedido
    ), 0)
""")

# === PAGAMENTOS ===
cursor.execute("SELECT id_pedido, data_pedido FROM tb_pedido WHERE status IN ('Faturado', 'Entregue')")
pagaveis = cursor.fetchall()
pagamentos = []
metodos = ["PIX", "Cartão de crédito", "Cartão de débito", "Boleto bancário", "Transferência bancária (TED)"]
for i, (pid, data) in enumerate(pagaveis):
    status = random.choices(["Pago", "Aguardando pagamento", "Cancelado"], weights=[0.8, 0.15, 0.05])[0]
    # data_pagamento agora inclui hora, baseada na data do pedido + um offset aleatório de dias e horas
    data_pagamento_com_hora = data + timedelta(days=random.randint(1, 5), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    pagamentos.append((pid, data_pagamento_com_hora, f"LS2025{i+1:05d}", round(random.uniform(100, 5000), 2), random.choice(metodos), status))
execute_values(cursor, """
    INSERT INTO tb_pagamento (id_pedido, data_pagamento, lote_saida, valor_pago, metodo_pagamento, status)
    VALUES %s
""", pagamentos)

# === FINALIZAÇÃO ===
conn.commit()
cursor.close()
conn.close()
print("População completa de 200.000 registros realizada com sucesso.")