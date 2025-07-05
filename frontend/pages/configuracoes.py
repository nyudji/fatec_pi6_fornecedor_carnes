import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import locale  # Importa módulo locale para formatação numérica
import subprocess
import sys
import os

def show():
    st.title("Configurações de Dados")

    st.header("Executar Scripts de Pipeline")

    col1, col2 = st.columns(2)

    # Caminho absoluto para a raiz do projeto
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    capture_script = os.path.join(project_root, "pipeline", "capture_web_data.py")
    export_script = os.path.join(project_root, "pipeline", "export_postgres_minio.py")

    with col1:
        if st.button("Rodar Capture Web Data"):
            with st.spinner("Executando capture_web_data.py..."):
                result = subprocess.run(
                    [sys.executable, capture_script],
                    capture_output=True, text=True
                )
                st.code(result.stdout)
                if result.stderr:
                    st.error(result.stderr)
                else:
                    st.success("Script capture_web_data.py executado com sucesso!")

    with col1:
        if st.button("Rodar Export Postgres Minio"):
            with st.spinner("Executando export_postgres_minio.py..."):
                result = subprocess.run(
                    [sys.executable, export_script],
                    capture_output=True, text=True
                )
                st.code(result.stdout)
                if result.stderr:
                    st.error(result.stderr)
                else:
                    st.success("Script export_postgres_minio.py executado com sucesso!")

