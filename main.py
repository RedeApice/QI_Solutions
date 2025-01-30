import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv()

Auth = os.getenv('AP')
endereco = os.getenv('URL')

headers = {
    'Authorization': Auth,
    'Accept': 'application/json'
}

url = endereco

response = requests.get(url, headers=headers)
data = response.json()

df = pd.DataFrame(data)

# df_2025 = df[df['ano_competencia'] == '2025']
df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce')

df_pago = df[df['valor_pago'].isnull() | (df['valor_pago'] == 0)]

# print(df_2025)
print(df_pago)

# df.to_csv('df_pago.csv', index=False)
# df.to_csv('df_2025.csv', index=False)

user = os.getenv('usuario')
password = os.getenv('senha')
hostname = os.getenv('host')
dbname = os.getenv('nome_banco')
table = os.getenv('tbl_name')

engine = create_engine(f'mysql+pymysql://{user}:{password}@{hostname}/{dbname}')

df = pd.read_csv('df_pago.csv')

df.to_sql(table, con=engine, if_exists='append', index=False)

print('Dados inseridos.')