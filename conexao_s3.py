import boto3
import os
import pandas as pd

# Configuração das credenciais da AWS
#aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
#aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
#region_name = "sa-east-1"

#boto3.setup_default_session(
#    aws_access_key_id=aws_access_key_id,
#    aws_secret_access_key=aws_secret_access_key,
#    region_name=region_name,
#)

s3 = boto3.client("s3")

# Caminho do arquivo CSV de entrada
csv_file_path = "data/IBOVDia_03-02-25.csv"

# Criando diretório de saída para arquivos Parquet, se não existir
parquet_dir = "data_parquet"
os.makedirs(parquet_dir, exist_ok=True)

# Convertendo CSV para Parquet e salvando em "data_parquet"
df = pd.read_csv(csv_file_path)  # Lendo CSV para um DataFrame do Pandas
parquet_file_path = os.path.join(parquet_dir, os.path.basename(csv_file_path).replace(".csv", ".parquet"))
df.to_parquet(parquet_file_path, engine="pyarrow", index=False)  # Salvando como Parquet

# Fazendo o upload do arquivo Parquet para o S3
bucket_name = "bucket-s3-b3-aws"
s3_key = f"bronze/{os.path.basename(parquet_file_path)}"  # Nome do arquivo no S3
s3.upload_file(parquet_file_path, bucket_name, s3_key)

print(f"Arquivo {parquet_file_path} enviado para {bucket_name}/{s3_key} com sucesso!")
print(f"O arquivo Parquet foi salvo localmente em {parquet_file_path}")
