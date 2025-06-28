from sidrapy import get_table

def consulta_abate_bovinos():
    dados = get_table(
        table_code='1092',
        territorial_level='1',         # nível Brasil
        ibge_territorial_code='all',   # todo o país
        variables='2368',              # número de cabeças abatidas
        periods='last'                 # último trimestre disponível
    )
    return dados  # já é um DataFrame do pandas

if __name__ == "__main__":
    resultado = consulta_abate_bovinos()
    for _, row in resultado.iterrows():
        print(f"{row['D1N']} - {row['D3N']} - {row['V']}")
