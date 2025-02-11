# Documentação do Código

## Visão Geral
Este script automatiza o download de arquivos CSV da B3 (Bolsa de Valores do Brasil), corrige erros de codificação no arquivo, converte-o para o formato Parquet e faz o upload do arquivo convertido para um bucket no Amazon S3.

## Dependências
O script utiliza as seguintes bibliotecas:
- `os`: Manipulação de diretórios e arquivos.
- `time`: Adiciona delays para aguardar processos assíncronos.
- `pytz`: Manipulação de fusos horários.
- `boto3`: Interação com AWS S3.
- `chardet`: Detecção de codificação de arquivos.
- `pandas`: Processamento de dados.
- `datetime`: Manipulação de datas e horas.
- `selenium`: Automatiza a navegação web e download de arquivos.

## Configuração Inicial
### Definição do fuso horário
```python
tz_brazil = pytz.timezone('America/Sao_Paulo')
```
Configura o fuso horário para São Paulo.

### Credenciais AWS
```python
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = "sa-east-1"
```

```python
aws_access_key_id = "###"
aws_secret_access_key = "###"
region_name = "sa-east-1"
```

Define as credenciais para acessar o Amazon S3.

## Funções Principais
### 1. Gerar Nome de Arquivo
```python
def gerar_nome_arquivo():
    data_atual = datetime.now(tz_brazil)
    nome_arquivo = f"IBOVDia_{data_atual.strftime('%d-%m-%y')}"
    return nome_arquivo
```
Gera um nome dinâmico para o arquivo CSV com base na data atual.

### 2. Registro de Logs
```python
def registrar_log(mensagem):
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, 'download_log.txt')
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        log_file.write(f"{mensagem}")
```
Registra eventos do download em um arquivo de log local.

```python
def registrar_log_aws(mensagem):
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, 'buckets3_log.txt')
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        log_file.write(f"{mensagem}")
```
Registra eventos de upload para o AWS S3.

### 3. Correção do Arquivo CSV
```python
def corrigir_csv(arquivo_entrada, arquivo_saida, ignorar_primeira_linha=True):
```
Corrige erros de codificação no arquivo CSV e remove a primeira linha, se necessário.

### 4. Verificação de Encoding
```python
def verificar_encoding(arquivo):
```
Detecta e imprime o encoding do arquivo CSV.

### 5. Conversão de CSV para Parquet
```python
def converter_para_parquet(csv_path, parquet_dir):
```
Converte o arquivo CSV corrigido para o formato Parquet.

## Configuração dos Diretórios
```python
download_dir = os.path.join(os.getcwd(), 'data')
parquet_dir = os.path.join(os.getcwd(), 'data_parquet')
os.makedirs(download_dir, exist_ok=True)
os.makedirs(parquet_dir, exist_ok=True)
```
Cria diretórios locais para armazenar os arquivos baixados e convertidos.

## Automalização com Selenium
### 1. Configuração do Chrome WebDriver
```python
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
```
Define opções para executar o navegador em modo headless e configurar o diretório de download.

### 2. Inicialização do WebDriver e Download do Arquivo
```python
driver = webdriver.Chrome(options=options)
driver.get('https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br')
wait = WebDriverWait(driver, 10)
download_button = wait.until(EC.presence_of_element_located((By.LINK_TEXT, 'Download')))
driver.execute_script("arguments[0].click();", download_button)
time.sleep(5)
```
Abre a página da B3 e baixa o arquivo CSV do índice IBOV.

### 3. Verifica se o Download Foi Concluído
```python
nome_arquivo_base = gerar_nome_arquivo()
nome_arquivo_csv = nome_arquivo_base + '.csv'
file_path = os.path.join(download_dir, nome_arquivo_csv)
```
Verifica se o arquivo foi baixado e registra no log.

### 4. Processamento do Arquivo Baixado
```python
verificar_encoding(file_path)
arquivo_corrigido_path = os.path.join(download_dir, nome_arquivo_csv)
sucesso = corrigir_csv(file_path, arquivo_corrigido_path)
```
Verifica e corrige a codificação do arquivo CSV.

### 5. Conversão e Upload para o AWS S3
```python
if sucesso:
    parquet_path = converter_para_parquet(arquivo_corrigido_path, parquet_dir)
    if parquet_path:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        s3.upload_file(
            parquet_path,
            "bucket-s3-b3-aws",
            f"bronze/{nome_arquivo_base}.parquet"
        )
        registrar_log_aws(f"O arquivo {nome_arquivo_base}.parquet foi registrado no Bucket S3\n")
```
Se o arquivo for corrigido e convertido com sucesso, ele é enviado para um bucket S3.

