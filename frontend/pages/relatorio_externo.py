import sys
import os
import subprocess

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from statsmodels.tsa.seasonal import seasonal_decompose

from datetime import datetime, timedelta
import locale  # Importa módulo locale para formatação numérica
from minio import Minio


minio_client = Minio(
    endpoint='localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

bucket = 'fornecedor-dados'

def download_df_from_minio(filename, minio_client, bucket):
    """
    Download de CSV do MinIO e retorna como DataFrame.
    """
    response = minio_client.get_object(bucket, filename)
    df = pd.read_csv(response, sep=',')
    response.close()
    response.release_conn()
    
    print(f"Arquivo {filename} baixado com sucesso do bucket {bucket}.")
    return df

df_cepea = download_df_from_minio('csv_exports/df_cepea.csv', minio_client, bucket)
df1_agro_gov = download_df_from_minio('csv_exports/df1_agro_gov.csv', minio_client, bucket) # Esse aqui
df2_agro_gov = download_df_from_minio('csv_exports/df2_agro_gov.csv', minio_client, bucket)
df3_agro_gov = download_df_from_minio('csv_exports/df3_agro_gov.csv', minio_client, bucket)

df = df1_agro_gov

# Prophet pode não estar instalado em todos os ambientes
try:
    from prophet import Prophet
    prophet_ok = True
except ImportError:
    prophet_ok = False

def show():
    st.title("Relatório Externo - Análise de Dados Agropecuários")

    # df já está carregado do MinIO
    st.header("Visualização inicial dos dados")
    st.write("Colunas e primeiras linhas:")
    st.dataframe(df.head(10))
    st.write("Últimas linhas:")
    st.dataframe(df.tail(10))

    st.subheader("Quantidade total de animais abatidos")
    total_abates = df["QUANTIDADE"].sum()    # ...imports e código anterior...
    
    from sklearn.linear_model import LinearRegression  # Deixe o import global, remova de dentro da função
    
    def show():
        st.title("Relatório Externo - Análise de Dados Agropecuários")
    
        # ...código anterior...
    
        # Regressão Linear (com normalização)
        st.header("Regressão Linear para Previsão Anual até 2030")
        df_ano = df.groupby('ANO')['QUANTIDADE'].sum().reset_index()
        df_ano['TimeIndex'] = np.arange(len(df_ano))
        X_lr = df_ano[['TimeIndex']]
        y_lr = df_ano['QUANTIDADE']
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_lr)
        model = LinearRegression()
        model.fit(X_scaled, y_lr)
        ano_max = df_ano['ANO'].max()
        anos_futuros = np.arange(ano_max + 1, 2031)
        time_index_futuro = np.arange(len(df_ano), len(df_ano) + len(anos_futuros)).reshape(-1, 1)
        time_index_futuro_scaled = scaler.transform(time_index_futuro)
        y_pred_futuro = model.predict(time_index_futuro_scaled)
        df_proj = pd.DataFrame({'ANO': anos_futuros, 'QUANTIDADE_PREDITA': y_pred_futuro.astype(int)})
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df_ano['ANO'], df_ano['QUANTIDADE'], label='Histórico')
        ax.plot(df_proj['ANO'], df_proj['QUANTIDADE_PREDITA'], '--', label='Previsão até 2030')
        ax.set_title('Previsão da QUANTIDADE de Animais Abatidos por Ano')
        ax.set_xlabel('Ano')
        ax.set_ylabel('Quantidade')
        ax.legend()
        ax.grid(True)
        st.pyplot(fig)  # Certifique-se de usar st.pyplot(fig) para mostrar o gráfico
    
        y_pred_hist = model.predict(X_scaled)
        st.write("Avaliação nos dados históricos:")
        st.write(f"MAE: {mean_absolute_error(y_lr, y_pred_hist):,.2f}")
        st.write(f"RMSE: {np.sqrt(mean_squared_error(y_lr, y_pred_hist)):.2f}")
        st.write(f"R²: {r2_score(y_lr, y_pred_hist):.2f}")
    
        # ...restante do código...
    st.write(f"Total de animais abatidos: {total_abates:,}")

    st.subheader("Total de Animais Abatidos por Ano")
    abates_por_ano = df.groupby("ANO")["QUANTIDADE"].sum()
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(x=abates_por_ano.index, y=abates_por_ano.values, palette="Blues_d", ax=ax)
    ax.set_title("Total de Animais Abatidos por Ano")
    ax.set_xlabel("Ano")
    ax.set_ylabel("Quantidade")
    plt.xticks(rotation=45)
    st.pyplot(fig)

    # Pré-processamento
    st.header("Pré-processamento e Agregação")
    df["DATA"] = pd.to_datetime(df["ANO"].astype(str) + "-" + df["MES"].astype(str) + "-01")
    df["MUNICIPIO_PROCEDENCIA"] = df["MUNICIPIO_PROCEDENCIA"].astype(str)
    df_agg = df.groupby(["DATA", "UF_PROCEDENCIA", "CATEGORIA"])["QUANTIDADE"].sum().reset_index()
    st.write("Amostra do DataFrame agregado:")
    st.dataframe(df_agg.head())

    # Random Forest (sem normalização)
    st.header("Random Forest para Previsão de QUANTIDADE")
    df_ml = df_agg.copy()
    df_ml["ANO"] = df_ml["DATA"].dt.year
    df_ml["MES"] = df_ml["DATA"].dt.month
    df_ml = pd.get_dummies(df_ml, columns=["UF_PROCEDENCIA", "CATEGORIA"], drop_first=True)
    X = df_ml.drop(columns=["QUANTIDADE", "DATA"])
    y = df_ml["QUANTIDADE"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    model_rf = RandomForestRegressor(n_estimators=100, random_state=42)
    model_rf.fit(X_train, y_train)
    y_pred_rf = model_rf.predict(X_test)
    st.write("Random Forest")
    st.write(f"MAE: {mean_absolute_error(y_test, y_pred_rf):,.2f}")
    st.write(f"RMSE: {np.sqrt(mean_squared_error(y_test, y_pred_rf)):.2f}")
    st.write(f"R²: {r2_score(y_test, y_pred_rf):.2f}")

    # Regressão Linear (com normalização)
    st.header("Regressão Linear para Previsão Anual até 2030")
    df_ano = df.groupby('ANO')['QUANTIDADE'].sum().reset_index()
    df_ano['TimeIndex'] = np.arange(len(df_ano))
    X_lr = df_ano[['TimeIndex']]
    y_lr = df_ano['QUANTIDADE']
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_lr)
    model = LinearRegression()
    model.fit(X_scaled, y_lr)
    ano_max = df_ano['ANO'].max()
    anos_futuros = np.arange(ano_max + 1, 2031)
    time_index_futuro = np.arange(len(df_ano), len(df_ano) + len(anos_futuros)).reshape(-1, 1)
    time_index_futuro_scaled = scaler.transform(time_index_futuro)
    y_pred_futuro = model.predict(time_index_futuro_scaled)
    df_proj = pd.DataFrame({'ANO': anos_futuros, 'QUANTIDADE_PREDITA': y_pred_futuro.astype(int)})
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df_ano['ANO'], df_ano['QUANTIDADE'], label='Histórico')
    ax.plot(df_proj['ANO'], df_proj['QUANTIDADE_PREDITA'], '--', label='Previsão até 2030')
    ax.set_title('Previsão da QUANTIDADE de Animais Abatidos por Ano')
    ax.set_xlabel('Ano')
    ax.set_ylabel('Quantidade')
    ax.legend()
    ax.grid(True)
    st.pyplot(fig)
    y_pred_hist = model.predict(X_scaled)
    st.write("Avaliação nos dados históricos:")
    st.write(f"MAE: {mean_absolute_error(y_lr, y_pred_hist):,.2f}")
    st.write(f"RMSE: {np.sqrt(mean_squared_error(y_lr, y_pred_hist)):.2f}")
    st.write(f"R²: {r2_score(y_lr, y_pred_hist):.2f}")

    # Prophet (série temporal)
    if prophet_ok:
        st.header("Previsão de Série Temporal com Prophet")
        df_prophet = df_agg.groupby("DATA")["QUANTIDADE"].sum().reset_index()
        df_prophet.columns = ["ds", "y"]
        modelo_prophet = Prophet()
        modelo_prophet.fit(df_prophet)
        futuro = modelo_prophet.make_future_dataframe(periods=12, freq="MS")
        forecast = modelo_prophet.predict(futuro)
        fig1 = modelo_prophet.plot(forecast)
        st.pyplot(fig1)
        fig2 = modelo_prophet.plot_components(forecast)
        st.pyplot(fig2)
    else:
        st.info("Prophet não está instalado. Pulei a previsão de série temporal.")

    # Clustering dos Estados (UFs)
    st.header("Clusterização dos Estados (UFs)")
    df_filtrado = df[df["ANO"] >= df["ANO"].max() - 4].copy()
    df_cluster = df_filtrado.groupby(["UF_PROCEDENCIA", "CATEGORIA"])["QUANTIDADE"].sum().unstack(fill_value=0)
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df_cluster)
    kmeans = KMeans(n_clusters=3, random_state=42)
    df_cluster["CLUSTER"] = kmeans.fit_predict(df_scaled)
    pca = PCA(n_components=3)
    pca_result = pca.fit_transform(df_scaled)
    df_cluster["PCA1"] = pca_result[:, 0]
    df_cluster["PCA2"] = pca_result[:, 1]
    df_cluster["PCA3"] = pca_result[:, 2]
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111, projection="3d")
    scatter = ax.scatter(df_cluster["PCA1"], df_cluster["PCA2"], df_cluster["PCA3"], c=df_cluster["CLUSTER"], cmap="viridis", s=100, edgecolors='k')
    for i, uf in enumerate(df_cluster.index):
        ax.text(df_cluster["PCA1"].iloc[i], df_cluster["PCA2"].iloc[i], df_cluster["PCA3"].iloc[i], uf, fontsize=9)
    ax.set_title("Clusterização dos Estados (PCA 3D)")
    ax.set_xlabel("PCA 1")
    ax.set_ylabel("PCA 2")
    ax.set_zlabel("PCA 3")
    st.pyplot(fig)

    # Clusterização simplificada por volume total
    st.header("Clusterização Simplificada por Volume Total de Abates")
    df_recente = df[df["ANO"] >= df["ANO"].max() - 4].copy()
    df_cluster2 = df_recente.groupby("UF_PROCEDENCIA")["QUANTIDADE"].sum().reset_index()
    df_cluster2.columns = ["UF", "TOTAL_ABATES"]
    scaler2 = StandardScaler()
    X2 = scaler2.fit_transform(df_cluster2[["TOTAL_ABATES"]])
    kmeans2 = KMeans(n_clusters=3, random_state=42)
    df_cluster2["CLUSTER"] = kmeans2.fit_predict(X2)
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(data=df_cluster2.sort_values("TOTAL_ABATES", ascending=False), x="UF", y="TOTAL_ABATES", hue="CLUSTER", palette="viridis", ax=ax)
    ax.set_title("Clusterização dos Estados por Volume Total de Abates (Últimos 5 anos)")
    ax.set_ylabel("Total de Abates")
    ax.set_xlabel("UF")
    plt.xticks(rotation=45)
    st.pyplot(fig)

    # Tendência de abates ao longo do tempo
    st.header("Tendência de Abates ao Longo do Tempo")
    serie_mensal = df.groupby("DATA")["QUANTIDADE"].sum().sort_index()
    resultado = seasonal_decompose(serie_mensal, model='additive', period=12)
    fig = resultado.plot()
    plt.suptitle("Decomposição de Série Temporal de Abates")
    plt.tight_layout()
    st.pyplot(fig)

    # Rolling Mean (média móvel)
    st.subheader("Tendência Suavizada (Média Móvel 12 meses)")
    fig, ax = plt.subplots(figsize=(12, 6))
    serie_mensal.rolling(window=12).mean().plot(label="Tendência (12 meses)", ax=ax)
    serie_mensal.plot(alpha=0.5, label="Original", ax=ax)
    ax.set_title("Tendência de Abates - Média Móvel")
    ax.legend()
    st.pyplot(fig)

    # Regressão Linear no Tempo
    st.subheader("Tendência Linear com Regressão")
    from sklearn.linear_model import LinearRegression
    serie = serie_mensal.reset_index()
    serie["DATA_NUM"] = (serie["DATA"] - serie["DATA"].min()).dt.days
    X3 = serie[["DATA_NUM"]]
    y3 = serie["QUANTIDADE"]
    modelo = LinearRegression()
    modelo.fit(X3, y3)
    serie["TREND"] = modelo.predict(X3)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(serie["DATA"], serie["QUANTIDADE"], label="Dados reais", alpha=0.6)
    ax.plot(serie["DATA"], serie["TREND"], label="Tendência linear", color="red", linewidth=2)
    ax.set_title("Tendência de Abates com Regressão Linear")
    ax.legend()
    ax.grid(True)
    st.pyplot(fig)

    # Prophet para tendência e anomalias
    if prophet_ok:
        st.subheader("Detecção de Anomalias com Prophet")
        df_prophet = serie[["DATA", "QUANTIDADE"]].rename(columns={"DATA": "ds", "QUANTIDADE": "y"})
        model = Prophet()
        model.fit(df_prophet)
        future = model.make_future_dataframe(periods=0)
        forecast = model.predict(future)
        df_prophet["previsto"] = forecast["yhat"]
        df_prophet["erro"] = abs(df_prophet["y"] - df_prophet["previsto"])
        limite = 2 * df_prophet["erro"].std()
        outliers = df_prophet[df_prophet["erro"] > limite]
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df_prophet["ds"], df_prophet["y"], label="Real")
        ax.plot(df_prophet["ds"], df_prophet["previsto"], label="Previsto")
        ax.scatter(outliers["ds"], outliers["y"], color="red", label="Anomalias")
        ax.set_title("Anomalias com Prophet")
        ax.legend()
        ax.grid(True)
        st.pyplot(fig)
    else:
        st.info("Prophet não está instalado. Pulei a detecção de anomalias.")

    # DB Score (Davies-Bouldin Index)
    st.header("Avaliação de Clusterização (DB Score)")
    from sklearn.metrics import davies_bouldin_score
    df_recente = df[df["ANO"] >= df["ANO"].max() - 4]
    df_cluster3 = df_recente.groupby(["UF_PROCEDENCIA", "CATEGORIA"])["QUANTIDADE"].sum().unstack(fill_value=0)
    scaler3 = StandardScaler()
    X3 = scaler3.fit_transform(df_cluster3)
    st.write("DB Score para diferentes valores de k (quanto menor, melhor):")
    for k in range(2, 10):
        model = KMeans(n_clusters=k, random_state=42)
        labels = model.fit_predict(X3)
        score = davies_bouldin_score(X3, labels)
        st.write(f"k={k} ➜ DB Score: {score:.3f}")

    st.success("Relatório completo gerado com sucesso!")