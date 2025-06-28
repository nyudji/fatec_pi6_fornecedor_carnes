# export_postgres_to_minio.py

from driver.psycopg2_connect import PostgresConnect
from minio import Minio
from dotenv import load_dotenv
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from io import BytesIO
import os
os.environ['MINIO_CLIENT_DISABLE_CERT_VERIFY'] = 'true'

# Carrega variáveis do .env
# Carregar variáveis do arquivo .env
# dotenv_path = find_dotenv()
# load_dotenv(dotenv_path)

# === Conexão com PostgreSQL usando sua classe ===
pg = PostgresConnect()
conn = pg.conn
cursor = pg.get_cursor()

minio_client = Minio(
    endpoint='localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False  # True se estiver usando HTTPS
)

bucket = 'fornecedor-dados'

# # === Conexão com o MinIO ===
# minio_client = Minio(
#     endpoint=os.getenv("MINIO_ENDPOINT"),
#     access_key=os.getenv("MINIO_ACCESS_KEY"),
#     secret_key=os.getenv("MINIO_SECRET_KEY"),
#     secure=False  # True se estiver usando HTTPS
# )

#bucket = os.getenv("MINIO_BUCKET")

# Cria bucket, se necessário
if not minio_client.bucket_exists(bucket):
    minio_client.make_bucket(bucket)

# === Obtem todas as tabelas públicas do PostgreSQL ===
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE'
""")
tabelas = [row[0] for row in cursor.fetchall()]

# === Para cada tabela, extrai dados e envia ao MinIO em formato Parquet ===
for tabela in tabelas:
    print(f"[INFO] Exportando tabela: {tabela}")
    
    try:
        df = pd.read_sql(f"SELECT * FROM {tabela}", conn)

        # Buffer em memória para o Parquet
        buffer = BytesIO()
        df.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)

        # Nome do arquivo no bucket
        object_name = f"postgres_exports/{tabela}.csv"

        # Enviar para MinIO
        minio_client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="text/csv"
        )

        print(f"[SUCESSO] {object_name} enviado para o bucket '{bucket}'")

    except Exception as e:
        print(f"[ERRO] Falha ao exportar a tabela '{tabela}': {e}")

# Fechar conexão
pg.close_connection()
