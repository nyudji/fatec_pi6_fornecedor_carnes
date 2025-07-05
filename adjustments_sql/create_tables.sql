-- Tabela de Fornecedores
CREATE TABLE tb_fornecedor (
    id_fornecedor SERIAL PRIMARY KEY,
    nome_fornecedor VARCHAR(255),
    cnpj_fornecedor VARCHAR(18) UNIQUE,
    telefone_fornecedor VARCHAR(20),
    email_fornecedor VARCHAR(100),
    endereco_fornecedor TEXT
);

-- Tabela de Produtos
CREATE TABLE tb_produto (
    id_produto SERIAL PRIMARY KEY,
    nome_produto VARCHAR(255),
    tipo_corte VARCHAR(100),
    unidade_medida VARCHAR(10),
    preco_compra NUMERIC(10,2),
    preco_venda NUMERIC(10,2),
    id_fornecedor INTEGER REFERENCES tb_fornecedor(id_fornecedor) ON DELETE SET NULL
);

-- Tabela de Clientes
CREATE TABLE tb_cliente (
    id_cliente SERIAL PRIMARY KEY,
    nome_cliente VARCHAR(255),
    cnpj_cliente VARCHAR(18) UNIQUE,
    telefone_cliente VARCHAR(20),
    email_cliente VARCHAR(100),
    endereco_cliente TEXT,
    tipo_cliente VARCHAR(50)
);

-- Tabela de Entradas (Notas de compra)
CREATE TABLE tb_entrada (
    id_entrada SERIAL PRIMARY KEY,
    data_entrada DATE,
    id_fornecedor INTEGER REFERENCES tb_fornecedor(id_fornecedor) ON DELETE SET NULL
);

-- Itens de entrada de produtos
CREATE TABLE tb_produto_entrada (
    id_item_entrada SERIAL PRIMARY KEY,
    id_entrada INTEGER REFERENCES tb_entrada(id_entrada) ON DELETE CASCADE,
    id_produto INTEGER REFERENCES tb_produto(id_produto) ON DELETE CASCADE,
    quantidade INTEGER,
    preco_total NUMERIC(10,2),
    validade DATE,
    lote VARCHAR(50)
);

-- Estoque com localização dos produtos
CREATE TABLE tb_estoque (
    id_estoque SERIAL PRIMARY KEY,
    item_entrada INTEGER REFERENCES tb_produto_entrada(id_item_entrada) ON DELETE CASCADE,
    quantidade_disponivel INTEGER,
    localizacao VARCHAR(100)
);

-- Pedidos dos clientes
CREATE TABLE tb_pedido (
    id_pedido SERIAL PRIMARY KEY,
    id_cliente INTEGER REFERENCES tb_cliente(id_cliente) ON DELETE SET NULL,
    data_pedido DATE,
    status VARCHAR(30),
    valor_total NUMERIC(10,2)
);

-- Itens dos pedidos
CREATE TABLE tb_item_pedido (
    id_item_pedido SERIAL PRIMARY KEY,
    id_pedido INTEGER REFERENCES tb_pedido(id_pedido) ON DELETE CASCADE,
    id_produto INTEGER REFERENCES tb_produto(id_produto) ON DELETE SET NULL,
    quantidade INTEGER,
    unidade_medida VARCHAR(10),
    preco_unitario NUMERIC(10,2)
);

-- Pagamentos dos pedidos
CREATE TABLE tb_pagamento (
    id_pagamento SERIAL PRIMARY KEY,
    id_pedido INTEGER REFERENCES tb_pedido(id_pedido) ON DELETE CASCADE,
    data_pagamento DATE,
    lote_saida VARCHAR(50),
    valor_pago NUMERIC(10,2),
    metodo_pagamento VARCHAR(50),
    status VARCHAR(30)
);
