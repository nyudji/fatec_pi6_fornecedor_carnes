# Reexecutando após reset: Parte 1 - Fornecedores e Clientes com regras aplicadas
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker('pt_BR')
random.seed(42)
np.random.seed(42)

# Endereços da região metropolitana de SP
bairros_sp = [
    "Mooca", "Pinheiros", "Barra Funda", "Santo André", "São Bernardo", "Guarulhos",
    "Tatuapé", "Ipiranga", "Liberdade", "Vila Mariana", "Santana", "São Caetano"
]

def endereco_sp():
    rua = fake.street_name()
    numero = fake.building_number()
    bairro = random.choice(bairros_sp)
    cep = fake.postcode()
    cidade = "São Paulo"
    return f"{rua}, {numero}, {bairro}, {cidade} - SP, {cep}"

# Fornecedores
def gerar_fornecedores(n=100):
    return pd.DataFrame([{
        'id_fornecedor': i + 1,
        'nome_fornecedor': fake.company(),
        'cnpj_fornecedor': fake.cnpj(),
        'telefone_fornecedor': f"(11) 9{random.randint(6000,9999)}-{random.randint(1000,9999)}",
        'email_fornecedor': fake.company_email(),
        'endereco_fornecedor': endereco_sp()
    } for i in range(n)])

# Clientes
def gerar_clientes(n=500):
    tipos = ['Atacado', 'Varejo', 'Restaurante', 'Mercado']
    return pd.DataFrame([{
        'id_cliente': i + 1,
        'nome_cliente': fake.name(),
        'cnpj_cliente': fake.cnpj(),
        'endereco_cliente': endereco_sp(),
        'telefone_cliente': f"(11) 9{random.randint(6000,9999)}-{random.randint(1000,9999)}",
        'email_cliente': fake.free_email(),
        'tipo_cliente': tipos[i % 4]
    } for i in range(n)])

# Gerar dados com regras aplicadas
df_fornecedores = gerar_fornecedores()
df_clientes = gerar_clientes()

# Visualizar amostras
df_fornecedores.head(), df_clientes.head()
