# Base para rodar fora do código

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import subprocess
import pandas as pd
from io import BytesIO
from datetime import datetime
from pipeline.capture_cepea import get_cepea_dataframe
from pipeline.capture_agro_gov import get_agro_gov_dataframes
from minio import Minio

def upload_df_to_minio(df, object_name, minio_client, bucket):
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    minio_client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="text/csv"
    )
    print(f"[SUCESSO] {object_name} enviado para o bucket '{bucket}'")

minio_client = Minio(
    endpoint='localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

bucket = 'fornecedor-dados'

# Baixa e converte o arquivo CEPEA
arquivo_baixado = get_cepea_dataframe()
downloads_dir = "/home/augustopinho/dev/clone_repository/fatec_pi6_fornecedor_carnes/downloads"

if arquivo_baixado:
    pasta_saida = os.path.dirname(arquivo_baixado)
    comando = [
        "libreoffice",
        "--headless",
        "--convert-to", "csv",
        arquivo_baixado,
        "--outdir", pasta_saida
    ]
    try:
        subprocess.run(comando, check=True)
    except Exception as e:
        print(f"Erro ao converter com LibreOffice: {e}")
else:
    print("Arquivo não foi baixado.")

# Lê o único arquivo CSV do diretório, usando a terceira linha como cabeçalho
arquivos = [f for f in os.listdir(downloads_dir) if f.endswith('.csv')]
if arquivos:
    csv_path = os.path.join(downloads_dir, arquivos[0])
    df = pd.read_csv(csv_path, encoding="latin1", header=2)
    print("Arquivo lido sucesso")
else:
    print("Nenhum arquivo CSV encontrado no diretório.")
    df = None

# Remove todos os arquivos do diretório
for f in os.listdir(downloads_dir):
    file_path = os.path.join(downloads_dir, f)
    if os.path.isfile(file_path):
        os.remove(file_path)

# Obtém os DataFrames do agro gov
df1_agro_gov, df2_agro_gov, df3_agro_gov = get_agro_gov_dataframes()

# Cria bucket, se necessário
if not minio_client.bucket_exists(bucket):
    minio_client.make_bucket(bucket)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Envia DataFrames para o MinIO
if df is not None:
    upload_df_to_minio(df, f"csv_exports/df_cepea.csv", minio_client, bucket)
upload_df_to_minio(df1_agro_gov, f"csv_exports/df1_agro_gov.csv", minio_client, bucket)
upload_df_to_minio(df2_agro_gov, f"csv_exports/df2_agro_gov.csv", minio_client, bucket)
upload_df_to_minio(df3_agro_gov, f"csv_exports/df3_agro_gov.csv", minio_client, bucket)