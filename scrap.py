import os
import time
import pytz
import boto3
import chardet
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

tz_brazil = pytz.timezone('America/Sao_Paulo')

#aws_access_key_id = "###"
#aws_secret_access_key = "###"
#region_name = "us-east-1"

def gerar_nome_arquivo():
    data_atual = datetime.now(tz_brazil)
    nome_arquivo = f"IBOVDia_{data_atual.strftime('%d-%m-%y')}"
    return nome_arquivo

def registrar_log(mensagem):
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, 'download_log.txt')
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        log_file.write(f"{mensagem}")

def registrar_log_aws(mensagem):
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, 'buckets3_log.txt')
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        log_file.write(f"{mensagem}")

def corrigir_csv(arquivo_entrada, arquivo_saida, ignorar_primeira_linha=True):
    try:
        with open(arquivo_entrada, 'rb') as file:
            raw_data = file.read()
            detected = chardet.detect(raw_data)
            encoding_original = detected['encoding']
        
        with open(arquivo_entrada, 'r', encoding=encoding_original) as file:
            linhas = file.readlines()
        
        if ignorar_primeira_linha and len(linhas) > 0:
            linhas = linhas[1:]
        
        mapeamento_caracteres = {
            'C�digo': 'Código',
            'A��o': 'Ação',
            'Te�rica': 'Teórica',
            '�': 'ã',
            '�': 'é',
            '�': 'ó',
            '�': 'í'
        }
        
        linhas_corrigidas = []
        for linha in linhas:
            linha_corrigida = linha
            for corrupto, correto in mapeamento_caracteres.items():
                linha_corrigida = linha_corrigida.replace(corrupto, correto)
            linhas_corrigidas.append(linha_corrigida)
        
        with open(arquivo_saida, 'w', encoding='utf-8', newline='') as file:
            file.writelines(linhas_corrigidas)
            
        return True
        
    except Exception as e:
        print(f"Erro ao processar o arquivo: {str(e)}")
        return False

def verificar_encoding(arquivo):
    with open(arquivo, 'rb') as file:
        raw_data = file.read()
        resultado = chardet.detect(raw_data)
        print(f"Encoding detectado: {resultado['encoding']}")
        print(f"Confiança da detecção: {resultado['confidence']:.2%}")

def converter_para_parquet(csv_path, parquet_dir):
    """
    Converte arquivo CSV para Parquet.
    
    Args:
        csv_path (str): Caminho do arquivo CSV
        parquet_dir (str): Diretório para salvar o arquivo Parquet
    
    Returns:
        str: Caminho do arquivo Parquet gerado
    """
    try:
        # Criar diretório se não existir
        os.makedirs(parquet_dir, exist_ok=True)
        
        # Ler CSV
        df = pd.read_csv(csv_path, encoding='utf-8')
        
        # Gerar nome do arquivo Parquet
        parquet_name = os.path.splitext(os.path.basename(csv_path))[0] + '.parquet'
        parquet_path = os.path.join(parquet_dir, parquet_name)
        
        # Salvar como Parquet
        df.to_parquet(parquet_path, index=False)
        
        print(f"Arquivo convertido para Parquet: {parquet_path}")
        return parquet_path
    except Exception as e:
        print(f"Erro ao converter para Parquet: {str(e)}")
        return None

# Configura os diretórios
download_dir = os.path.join(os.getcwd(), 'data')
parquet_dir = os.path.join(os.getcwd(), 'data_parquet')
os.makedirs(download_dir, exist_ok=True)
os.makedirs(parquet_dir, exist_ok=True)

# Configurações do Chrome
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_experimental_option('prefs', {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

# Inicializa o navegador
driver = webdriver.Chrome(options=options)

try:
    # Abre a página da B3
    driver.get('https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br')

    # Espera e clica no botão de download
    wait = WebDriverWait(driver, 10)
    download_button = wait.until(EC.presence_of_element_located((By.LINK_TEXT, 'Download')))
    driver.execute_script("arguments[0].click();", download_button)

    # Aguarda o download
    time.sleep(5)

    # Verifica o download
    nome_arquivo_base = gerar_nome_arquivo()
    nome_arquivo_csv = nome_arquivo_base + '.csv'
    file_path = os.path.join(download_dir, nome_arquivo_csv)

    if os.path.exists(file_path):
        mensagem_log = f"\nO arquivo da data {datetime.now(tz_brazil).strftime('%d-%m-%y')} foi baixado"
    else:
        mensagem_log = f"O download do arquivo CSV falhou: {nome_arquivo_csv}"

    registrar_log(mensagem_log)
    
    # Verificar o encoding atual do arquivo
    verificar_encoding(file_path)

    # Corrigir o arquivo e remover a primeira linha
    arquivo_corrigido_path = os.path.join(download_dir, nome_arquivo_csv)
    sucesso = corrigir_csv(file_path, arquivo_corrigido_path)
    if sucesso:
        print(f"O arquivo CSV foi corrigido com sucesso: {arquivo_corrigido_path}")
        
        # Converter para Parquet
        parquet_path = converter_para_parquet(arquivo_corrigido_path, parquet_dir)
        if parquet_path:
            # Upload para S3
            s3 = boto3.client(
                "s3",
                #aws_access_key_id=aws_access_key_id,
                #aws_secret_access_key=aws_secret_access_key,
                #region_name=region_name
            )
            
            s3.upload_file(
                parquet_path,
                "bucket-s3-b3-aws",
                f"bronze/{nome_arquivo_base}.parquet"
            )
            
            mensagem_log2 = f"O arquivo {nome_arquivo_base}.parquet foi registrado no Bucket S3\n"
            registrar_log_aws(mensagem_log2)
    else:
        print("Falha ao corrigir o arquivo CSV")
        mensagem_log2 = f"A inserção no Bucket S3 falhou: {nome_arquivo_csv}\n"
        registrar_log_aws(mensagem_log2)

finally:
    driver.quit()