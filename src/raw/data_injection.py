import os
import pandas as pd
from sqlalchemy import create_engine

db_url = "postgresql+psycopg2://admin:secretpassword@postgres_source:5432/transito_db"
engine = create_engine(db_url)

print("Lendo os arquivos CSV com Pandas (delimitador correto ';')...")

df_acidentes = pd.read_csv('/opt/airflow/src/raw/data/acidentes2026.csv', encoding='latin-1', sep=';', low_memory=False)
df_datatran = pd.read_csv('/opt/airflow/src/raw/data/datatran2026.csv', encoding='latin-1', sep=';', low_memory=False)

print(f"Total de registros em acidentes: {len(df_acidentes)}")
print(f"Total de registros em datatran: {len(df_datatran)}")

print("Salvando 'tb_acidentes' no PostgreSQL...")
df_acidentes.to_sql('tb_acidentes', engine, if_exists='replace', index=False, chunksize=10000)

print("Salvando 'tb_datatran' no PostgreSQL...")
df_datatran.to_sql('tb_datatran', engine, if_exists='replace', index=False, chunksize=10000)

print("Carga inicial concluída com sucesso no PostgreSQL!")