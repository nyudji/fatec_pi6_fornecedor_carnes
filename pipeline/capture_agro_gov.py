import pandas as pd

# URLs fixas
URLS = [
    "https://dados.agricultura.gov.br/dataset/062166e3-b515-4274-8e7d-68aadd64b820/resource/239eaa90-35cd-4b67-8902-d34eda3dca53/download/sigsifquantitativoanimaisabatidoscategoriauf.csv",
    "https://dados.agricultura.gov.br/dataset/062166e3-b515-4274-8e7d-68aadd64b820/resource/8c2cc427-bb38-4341-8b6f-a397a5f2da5c/download/sigsifcondenacaoanimaisporespecie.csv",
    "https://dados.agricultura.gov.br/dataset/062166e3-b515-4274-8e7d-68aadd64b820/resource/341dc717-4716-42ab-b189-c8d7a9d2a1ba/download/sigsifrelatorioabates.csv"
]

def get_agro_gov_dataframes():
    """
    Lê os 3 arquivos CSV do governo diretamente das URLs e retorna 3 dataframes fixos.
    """
    dfs = []
    for url in URLS:
        try:
            df = pd.read_csv(url, encoding="utf-8", sep=None, engine="python")
            dfs.append(df)
            print(f"Sucesso ao ler {url}: {df.shape[0]} linhas, {df.shape[1]} colunas")
        except Exception as e:
            print(f"Erro ao ler {url}: {e}")
            dfs.append(None)
    # Retorna três variáveis separadas
    return tuple(dfs)