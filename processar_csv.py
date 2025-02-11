import csv
import chardet

def corrigir_csv(arquivo_entrada, arquivo_saida, ignorar_primeira_linha=True):
    """
    Corrige problemas de encoding em arquivos CSV e opcionalmente ignora a primeira linha.
    
    Args:
        arquivo_entrada (str): Caminho do arquivo CSV original
        arquivo_saida (str): Caminho onde será salvo o arquivo corrigido
        ignorar_primeira_linha (bool): Se True, remove a primeira linha do arquivo
    
    Returns:
        bool: True se a operação foi bem sucedida, False caso contrário
    """
    try:
        # Detecta o encoding do arquivo
        with open(arquivo_entrada, 'rb') as file:
            raw_data = file.read()
            detected = chardet.detect(raw_data)
            encoding_original = detected['encoding']
        
        # Lê o arquivo com o encoding detectado
        with open(arquivo_entrada, 'r', encoding=encoding_original) as file:
            linhas = file.readlines()
        
        # Remove a primeira linha se solicitado
        if ignorar_primeira_linha and len(linhas) > 0:
            linhas = linhas[1:]
        
        # Mapeamento de caracteres corrompidos comuns
        mapeamento_caracteres = {
            'C�digo': 'Código',
            'A��o': 'Ação',
            'Te�rica': 'Teórica',
            '�': 'ã',
            '�': 'é',
            '�': 'ó',
            '�': 'í'
        }
        
        # Aplica as correções em cada linha
        linhas_corrigidas = []
        for linha in linhas:
            linha_corrigida = linha
            for corrupto, correto in mapeamento_caracteres.items():
                linha_corrigida = linha_corrigida.replace(corrupto, correto)
            linhas_corrigidas.append(linha_corrigida)
        
        # Salva o arquivo corrigido em UTF-8
        with open(arquivo_saida, 'w', encoding='utf-8', newline='') as file:
            file.writelines(linhas_corrigidas)
            
        return True
        
    except Exception as e:
        print(f"Erro ao processar o arquivo: {str(e)}")
        return False

def verificar_encoding(arquivo):
    """
    Verifica e exibe informações sobre o encoding do arquivo.
    
    Args:
        arquivo (str): Caminho do arquivo a ser verificado
    """
    with open(arquivo, 'rb') as file:
        raw_data = file.read()
        resultado = chardet.detect(raw_data)
        print(f"Encoding detectado: {resultado['encoding']}")
        print(f"Confiança da detecção: {resultado['confidence']:.2%}")

# Verificar o encoding atual do arquivo
verificar_encoding('data/IBOVDia_04-02-25.csv')

# Corrigir o arquivo e remover a primeira linha
sucesso = corrigir_csv('data/IBOVDia_04-02-25.csv', 'data/IBOVDia_04-02-25.csv')