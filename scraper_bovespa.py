import pandas as pd
import boto3
from datetime import datetime
import io
import os

def extrair_dados_b3_e_enviar_s3():
    """
    Extrai a tabela de dados do índice IBOV do site da B3,
    converte para o formato Parquet e faz o upload para um bucket S3
    com particionamento por data (ano/mes/dia).
    """
    # URL fornecida para extração dos dados
    URL_BOVESPA = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
    
    # Nome do seu bucket S3 para dados brutos. 
    # É uma boa prática usar variáveis de ambiente para isso.
    S3_BUCKET_NAME = os.environ.get("S3_RAW_BUCKET", "seu-bucket-de-dados-crus-aqui")

    print(f"Iniciando extração da URL: {URL_BOVESPA}")

    try:
        # O pandas lê o HTML e retorna uma lista de todas as tabelas encontradas
        tabelas = pd.read_html(URL_BOVESPA, decimal=',', thousands='.')
        
        # Verificamos se alguma tabela foi encontrada
        if not tabelas:
            print("Nenhuma tabela encontrada na página.")
            return

        # A tabela que nos interessa geralmente é a primeira
        df = tabelas[0]
        print("Tabela extraída com sucesso. Exemplo:")
        print(df.head())
        
        # Captura a data atual para o particionamento
        hoje = datetime.now()
        ano = hoje.year
        mes = hoje.month
        dia = hoje.day

        # Define o caminho (chave) do objeto no S3 com as partições
        s3_key = f"ano={ano}/mes={mes}/dia={dia}/dados_bovespa.parquet"

        # Converte o DataFrame para o formato Parquet em memória
        buffer_parquet = io.BytesIO()
        df.to_parquet(buffer_parquet, index=False, engine='fastparquet')
        buffer_parquet.seek(0) # Reposiciona o cursor para o início do buffer

        # Inicia o cliente S3
        s3_client = boto3.client('s3')

        print(f"Enviando arquivo para o S3: s3://{S3_BUCKET_NAME}/{s3_key}")
        
        # Faz o upload do buffer em memória para o S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=buffer_parquet
        )
        
        print("Upload para o S3 concluído com sucesso!")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    extrair_dados_b3_e_enviar_s3()