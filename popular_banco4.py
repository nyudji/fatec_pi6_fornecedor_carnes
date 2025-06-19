# popular_banco.py
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

# --- CONFIGURAÇÃO DE BATCH SIZES ---
BATCH_SIZE_CLIENTES = 1000
BATCH_SIZE_ENTRADAS = 2000
BATCH_SIZE_PRODUTO_ENTRADA = 5000
BATCH_SIZE_ESTOQUE = 5000
BATCH_SIZE_PEDIDOS = 5000 # Lotes para pedidos e itens relacionados
BATCH_SIZE_PAGAMENTOS = 10000

# --- Controle de avisos de estoque ---
MAX_ESTOQUE_ESGOTADO_WARNINGS = 5 # Limite de avisos de estoque esgotado
estoque_esgotado_warnings_count = 0
estoque_global_esgotado = False # Flag para saber se o estoque geral acabou

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
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"   {len(batch)} clientes inseridos. Total: {i + len(batch)}/{num_clientes}")
print(f"Total de {num_clientes} clientes inseridos.")

# --- ENTRADAS E PRODUTO_ENTRADA ---
print("Gerando e inserindo entradas e produtos de entrada...")
cursor.execute("SELECT id_produto, preco_compra FROM tb_produto")
produtos_info = cursor.fetchall() # (id_produto, preco_compra)

num_entradas = 5000
entradas_data_list = []
for _ in range(num_entradas):
    data = datetime.now() - timedelta(days=random.randint(1, 90), hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    entradas_data_list.append((data, id_fornecedor_fixo))

inserted_entrada_ids = [] # (id_entrada, data_entrada)
for i in range(0, len(entradas_data_list), BATCH_SIZE_ENTRADAS):
    batch = entradas_data_list[i:i + BATCH_SIZE_ENTRADAS]
    execute_values(cursor, """
        INSERT INTO tb_entrada (data_entrada, id_fornecedor)
        VALUES %s
        RETURNING id_entrada, data_entrada
    """, batch)
    inserted_entrada_ids.extend(cursor.fetchall())
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"   {len(batch)} entradas inseridas. Total: {i + len(batch)}/{num_entradas}")

produtos_entrada_data_list = []
itens_para_estoque_creation = [] # (id_item_entrada_retornado, quantidade_original)
for entrada_id, data_entrada in inserted_entrada_ids:
    for _ in range(random.randint(1, 4)): # 1 a 4 produtos por entrada
        prod_id, preco_compra = random.choice(produtos_info)
        qtd = random.randint(50, 300) # Quantidade comprada na entrada
        total = round(qtd * preco_compra, 2)
        validade = data_entrada + timedelta(days=random.randint(15, 60)) # Validade mais curta para carnes
        lote = f"L{data_entrada.strftime('%Y%m%d')}P{prod_id:03d}E{entrada_id}" # Lote mais descritivo
        produtos_entrada_data_list.append((entrada_id, prod_id, qtd, total, validade, lote))

# Inserir produtos de entrada em lotes e coletar IDs e quantidades para estoque
for i in range(0, len(produtos_entrada_data_list), BATCH_SIZE_PRODUTO_ENTRADA):
    batch = produtos_entrada_data_list[i:i + BATCH_SIZE_PRODUTO_ENTRADA]
    execute_values(cursor, """
        INSERT INTO tb_produto_entrada (id_entrada, id_produto, quantidade, preco_total, validade, lote)
        VALUES %s
        RETURNING id_item_entrada, quantidade
    """, batch)
    itens_para_estoque_creation.extend(cursor.fetchall()) # Collect (id_item_entrada, quantidade_original_entrada)
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"   {len(batch)} produtos de entrada inseridos.")
print(f"Total de {len(produtos_entrada_data_list)} produtos de entrada inseridos.")

# --- ESTOQUE ---
print("Gerando e inserindo estoque...")
estoque_data_list = []
estoque_atual = {} # {id_item_entrada: {'quantidade_disponivel': X, 'id_produto': Y, 'preco_venda': Z, 'unidade_medida': W}}

for item_entrada_id, quantidade_original in itens_para_estoque_creation:
    quantidade_disponivel = random.randint(quantidade_original // 2, quantidade_original)
    localizacao = random.choice(["Câmara Fria 1", "Freezer", "Prateleira A1", "Despacho"])
    
    estoque_data_list.append((item_entrada_id, quantidade_disponivel, localizacao))
    
    cursor.execute("""
        SELECT p.id_produto, p.preco_venda, p.unidade_medida
        FROM tb_produto_entrada pe
        JOIN tb_produto p ON p.id_produto = pe.id_produto
        WHERE pe.id_item_entrada = %s
    """, (item_entrada_id,))
    prod_info = cursor.fetchone()
    
    if prod_info:
        estoque_atual[item_entrada_id] = {
            'quantidade_disponivel': quantidade_disponivel,
            'id_produto': prod_info[0],
            'preco_venda': float(prod_info[1]),
            'unidade_medida': prod_info[2]
        }
    # else: Não precisa de aviso aqui, pois se não encontrou, não será adicionado ao estoque_atual

for i in range(0, len(estoque_data_list), BATCH_SIZE_ESTOQUE):
    batch = estoque_data_list[i:i + BATCH_SIZE_ESTOQUE]
    execute_values(cursor, """
        INSERT INTO tb_estoque (item_entrada, quantidade_disponivel, localizacao)
        VALUES %s
    """, batch)
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"   {len(batch)} itens de estoque inseridos. Total: {i + len(batch)}/{len(estoque_data_list)}")
print(f"Total de {len(estoque_data_list)} itens de estoque inseridos.")


# --- PEDIDOS E ITENS DO PEDIDO (COM BAIXA DE ESTOQUE CONTROLADA) ---
print("Gerando e inserindo pedidos e seus itens (com cálculo de valor_total e baixa de estoque controlada)...")
num_pedidos = 50000
all_inserted_pedido_ids = []

# IDs de itens de estoque que ainda têm quantidade disponível
disponivel_estoque_ids = list(estoque_atual.keys())
random.shuffle(disponivel_estoque_ids)

for i in range(0, num_pedidos, BATCH_SIZE_PEDIDOS):
    pedidos_batch_data = []
    itens_pedido_batch_data = []
    current_batch_pedido_map = {}
    
    if estoque_global_esgotado:
        # Se o estoque global já foi marcado como esgotado, para de gerar pedidos e itens
        print("Aviso: Estoque global esgotado. Parando a geração de novos pedidos e itens de pedido.")
        break # Sai do loop principal de pedidos

    # Geração de dados para um lote completo de pedidos e seus itens
    for _ in range(min(BATCH_SIZE_PEDIDOS, num_pedidos - i)):
        data_pedido = datetime.now() - timedelta(days=random.randint(1, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        status = random.choices(
            ["Pendente", "Faturado", "Entregue", "Cancelado"],
            weights=[0.3, 0.3, 0.35, 0.05],
            k=1
        )[0]
        
        current_pedido_total = 0.0
        current_pedido_items_to_add = [] # (produto_id, qtd, unidade_medida, preco_unitario)
        
        num_itens_por_pedido = random.randint(2, 5)
        
        for _ in range(num_itens_por_pedido):
            if not disponivel_estoque_ids:
                if estoque_esgotado_warnings_count < MAX_ESTOQUE_ESGOTADO_WARNINGS:
                    print("Aviso: Estoque simulado esgotado. Próximos itens de pedido podem ser omitidos ou gerados sem coerência de estoque.")
                    estoque_esgotado_warnings_count += 1
                estoque_global_esgotado = True # Marca o estoque global como esgotado
                break # Sai do loop de itens para este pedido
            
            # Tentar escolher um item de estoque disponível
            chosen_item_entrada_id = random.choice(disponivel_estoque_ids)
            
            # Remover itens de estoque que esgotaram ou foram removidos de estoque_atual
            while (chosen_item_entrada_id not in estoque_atual or estoque_atual[chosen_item_entrada_id]['quantidade_disponivel'] <= 0) and disponivel_estoque_ids:
                disponivel_estoque_ids.remove(chosen_item_entrada_id)
                if disponivel_estoque_ids:
                    chosen_item_entrada_id = random.choice(disponivel_estoque_ids)
                else:
                    if estoque_esgotado_warnings_count < MAX_ESTOQUE_ESGOTADO_WARNINGS:
                        print("Aviso: Estoque simulado esgotado. Próximos itens de pedido podem ser omitidos ou gerados sem coerência de estoque.")
                        estoque_esgotado_warnings_count += 1
                    estoque_global_esgotado = True
                    break # Sai do loop while
            
            if estoque_global_esgotado: # Se o estoque esgotou durante a tentativa, pula
                break
                
            item_estoque_info = estoque_atual[chosen_item_entrada_id]
            max_qtd_pedido = min(item_estoque_info['quantidade_disponivel'], 20)
            
            if max_qtd_pedido > 0:
                qtd_pedida = random.randint(1, max_qtd_pedido)
                
                # Simula a baixa no estoque em memória
                estoque_atual[chosen_item_entrada_id]['quantidade_disponivel'] -= qtd_pedida
                
                preco_unitario = item_estoque_info['preco_venda']
                produto_id = item_estoque_info['id_produto']
                unidade_medida = item_estoque_info['unidade_medida']
                
                item_total = float(round(qtd_pedida * preco_unitario, 2))
                current_pedido_total += item_total
                current_pedido_items_to_add.append((produto_id, qtd_pedida, unidade_medida, preco_unitario))

                if estoque_atual[chosen_item_entrada_id]['quantidade_disponivel'] <= 0:
                    # Remove do pool de IDs disponíveis se esgotou completamente
                    if chosen_item_entrada_id in disponivel_estoque_ids:
                        disponivel_estoque_ids.remove(chosen_item_entrada_id)
            else:
                # Este caso deve ser capturado pelo loop while acima, mas como precaução
                if estoque_esgotado_warnings_count < MAX_ESTOQUE_ESGOTADO_WARNINGS:
                    print("Aviso: Item de estoque encontrado, mas sem quantidade disponível. Pulando item.")
                    estoque_esgotado_warnings_count += 1
                if chosen_item_entrada_id in disponivel_estoque_ids:
                    disponivel_estoque_ids.remove(chosen_item_entrada_id)

        # Se não adicionou nenhum item ao pedido (ex: estoque esgotou durante a geração), pula este pedido
        if not current_pedido_items_to_add:
            continue
            
        pedidos_batch_data.append((random.randint(1, num_clientes), data_pedido, status, round(current_pedido_total, 2)))
        current_batch_pedido_map[len(pedidos_batch_data) - 1] = current_pedido_items_to_add


    # Inserção do lote de pedidos
    if not pedidos_batch_data: # Se não há pedidos para inserir neste lote (ex: estoque esgotado)
        continue # Pula para a próxima iteração do loop principal

    execute_values(cursor, """
        INSERT INTO tb_pedido (id_cliente, data_pedido, status, valor_total)
        VALUES %s
        RETURNING id_pedido
    """, pedidos_batch_data)
    current_inserted_pedido_ids = cursor.fetchall()
    all_inserted_pedido_ids.extend([r[0] for r in current_inserted_pedido_ids])
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"   {len(pedidos_batch_data)} pedidos inseridos. Total: {i + len(pedidos_batch_data)}/{num_pedidos}")

    # Geração e inserção dos itens de pedido
    for idx_in_batch, (actual_pid,) in enumerate(current_inserted_pedido_ids):
        simulated_itens = current_batch_pedido_map.get(idx_in_batch, [])
        for prod_id, qtd, unidade_medida, preco_unitario in simulated_itens:
            itens_pedido_batch_data.append((actual_pid, prod_id, qtd, unidade_medida, preco_unitario))
    
    if itens_pedido_batch_data:
        execute_values(cursor, """
            INSERT INTO tb_item_pedido (id_pedido, id_produto, quantidade, unidade_medida, preco_unitario)
            VALUES %s
        """, itens_pedido_batch_data)
        db_connection.commit() # Usa o método de commit da sua classe
        print(f"     {len(itens_pedido_batch_data)} itens de pedido inseridos para o lote atual.")
    else:
        print("     Nenhum item de pedido gerado para o lote atual devido a restrições de estoque.")


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
    referencia_pagamento = f"REF{datetime.now().strftime('%Y%m%d%H%M%S')}{i:05d}"
    valor_pago = round(random.uniform(100, 5000), 2)
    pagamentos_data_list.append((pid, data_pagamento_com_hora, referencia_pagamento, valor_pago, random.choice(metodos), status))

for i in range(0, len(pagamentos_data_list), BATCH_SIZE_PAGAMENTOS):
    batch = pagamentos_data_list[i:i + BATCH_SIZE_PAGAMENTOS]
    execute_values(cursor, """
        INSERT INTO tb_pagamento (id_pedido, data_pagamento, lote_saida, valor_pago, metodo_pagamento, status)
        VALUES %s
    """, batch)
    db_connection.commit() # Usa o método de commit da sua classe
    print(f"   {len(batch)} pagamentos inseridos.")
print(f"Total de {len(pagamentos_data_list)} pagamentos inseridos.")

# --- FINALIZAÇÃO ---
# cursor.close() # O fechamento do cursor é gerenciado por db_connection.close_connection()
db_connection.close_connection() # Fecha a conexão e o cursor associado
end_time = time.time()
print(f"\nPopulação completa de registros realizada com sucesso em {end_time - start_time:.2f} segundos.")