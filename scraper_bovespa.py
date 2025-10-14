import logging
import io
from datetime import datetime
import time

import pandas as pd
import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_b3_html_with_selenium():
    """
    Usa o Selenium para carregar a página da B3, aceitar o banner de cookies,
    aguardar a tabela dinâmica via XPath e retornar o HTML completo da página.
    """
    url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/G/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    service = ChromeService(ChromeDriverManager().install())
    driver = None
    try:
        logging.info(f"Iniciando o navegador e acessando a URL: {url}")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.get(url)

        try:
            cookie_button_id = "onetrust-accept-btn-handler"
            logging.info(f"Procurando pelo botão de aceitar cookies com ID: {cookie_button_id}")
            cookie_button = WebDriverWait(driver, 5).until( # Reduzido o tempo de espera do cookie
                EC.element_to_be_clickable((By.ID, cookie_button_id))
            )
            logging.info("Botão de cookies encontrado. Clicando para aceitar.")
            cookie_button.click()
            time.sleep(2)
        except TimeoutException:
            logging.warning("Banner de cookies não encontrado ou já aceito. Prosseguindo...")

        # --- ALTERAÇÃO PRINCIPAL AQUI ---
        # Em vez de procurar por um ID fixo, procuramos pela tabela usando seu conteúdo (cabeçalho)
        table_xpath = "//table[.//th[contains(text(), 'Qtde. Teórica')]]"
        logging.info(f"Aguardando o carregamento da tabela dinâmica via XPath...")
        
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, table_xpath))
        )
        
        logging.info("Tabela encontrada com sucesso via XPath. Capturando o HTML da página.")
        html_content = driver.page_source
        return html_content

    except TimeoutException:
        logging.error("A tabela não foi encontrada na página via XPath após aguardar. O layout do site pode ter mudado drasticamente.")
        driver.save_screenshot("debug_screenshot_final_error.png")
        logging.info("Um screenshot 'debug_screenshot_final_error.png' foi salvo para análise.")
        return None
    except Exception as e:
        logging.error(f"Erro inesperado ao buscar dados com Selenium: {e}")
        return None
    finally:
        if driver:
            driver.quit()
            logging.info("Navegador finalizado.")

def parse_html_to_dataframe(html_content):
    if not html_content: return None
    try:
        tables = pd.read_html(io.StringIO(html_content), decimal=',', thousands='.')
        df = None
        for table in tables:
            if 'Código' in table.columns and 'Ação' in table.columns:
                df = table
                break
        if df is None:
            logging.warning("Nenhuma tabela com as colunas esperadas ('Código', 'Ação') foi encontrada no HTML.")
            return None
        logging.info("Tabela HTML convertida para DataFrame com sucesso.")
        df['dt_extracao'] = pd.to_datetime(datetime.now().isoformat())
        return df
    except Exception as e:
        logging.error(f"Erro ao parsear o HTML com pandas: {e}")
        return None

def upload_to_s3(df, bucket_name):
    if df is None or df.empty:
        logging.warning("DataFrame vazio. Nenhum dado será enviado para o S3.")
        return
    try:
        now = datetime.now()
        year = now.strftime("%Y"); month = now.strftime("%m"); day = now.strftime("%d")
        s3_key = f"year={year}/month={month}/day={day}/dados_bovespa.parquet"
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow', use_deprecated_int96_timestamps=True)
        s3_client = boto3.client('s3')
        parquet_buffer.seek(0)
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_buffer.getvalue())
        logging.info(f"Arquivo salvo com sucesso em s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logging.error(f"Erro ao fazer upload para o S3: {e}")

if __name__ == "__main__":
    S3_BUCKET_NAME = "bucket-extract-techchallenge"
    html = fetch_b3_html_with_selenium()
    if html:
        dataframe = parse_html_to_dataframe(html)
        if dataframe is not None:
            upload_to_s3(dataframe, S3_BUCKET_NAME)