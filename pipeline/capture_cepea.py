import sys
import os
import time
import pandas as pd
import io
import mimetypes

def get_cepea_dataframe():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    download_dir = os.path.join(project_root, "downloads")
    os.makedirs(download_dir, exist_ok=True)  # Garante que a pasta existe

    from selenium import webdriver
    from selenium.webdriver.firefox.service import Service
    from selenium.webdriver.firefox.options import Options
    from webdriver_manager.firefox import GeckoDriverManager
    from selenium.webdriver.common.by import By

    options = Options()
    # options.add_argument("--headless")

    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.download.folderList", 2)
    profile.set_preference("browser.download.dir", download_dir)
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/vnd.ms-excel,application/octet-stream,application/csv,text/csv,application/zip,application/x-zip-compressed,application/pdf")
    profile.set_preference("pdfjs.disabled", True)
    options.profile = profile

    driver = webdriver.Firefox(
        service=Service(GeckoDriverManager().install()),
        options=options
    )

    try:
        url = "https://www.cepea.org.br/br/indicador/boi-gordo.aspx#imagenet-popup-grafico1"
        driver.get(url)
        time.sleep(5)

        arquivos_antes = set(os.listdir(download_dir))
        xpath = "/html/body/div/div[3]/div[2]/div[2]/div[2]/div[1]/div[3]/a[4]"
        botao = driver.find_element(By.XPATH, xpath)
        botao.click()
        time.sleep(10)  # aumente o tempo de espera

        arquivos_depois = set(os.listdir(download_dir))
        novos_arquivos = arquivos_depois - arquivos_antes

        if novos_arquivos:
            arquivo_baixado = max(
                [os.path.join(download_dir, f) for f in novos_arquivos],
                key=os.path.getctime
            )
            # Agora só retorna o caminho do arquivo baixado
            return arquivo_baixado
        else:
            print("Nenhum arquivo novo encontrado no diretório de download.")
            return None
    finally:
        driver.quit()




