import pandas as pd
import requests
import pymysql
from dotenv import load_dotenv
import os

load_dotenv()
Auth = os.getenv('AP')
endereco = os.getenv('URL')
usuario = os.getenv('usuario')
senha = os.getenv('senha')
host = os.getenv('host')
nome_banco = os.getenv('nome_banco')
tbl_name = os.getenv('tbl_name')

connection = pymysql.connect(host=host, user=usuario, password=senha, db=nome_banco)
cursor = connection.cursor()

cursor.execute(f'''
CREATE TABLE IF NOT EXISTS {tbl_name} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data_pagamento DATE,
    valor_pago DECIMAL(10,2),
    observacoes VARCHAR(255)
)
''')

headers = {
    'Authorization': Auth,
    'Accept': 'application/json'
}
response = requests.get(endereco, headers=headers)
response.raise_for_status()
data = response.json()

df = pd.DataFrame(data)
df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce')

df_filtrado = df[df['valor_pago'].isna()]

sql = f'''
INSERT INTO {tbl_name} (data_pagamento, valor_pago, observacoes) 
VALUES (%s, %s, %s)
'''

for _, row in df_filtrado.iterrows():
    cursor.execute(sql, (row.get('data_pagamento'), row['valor_pago'], row.get('observacoes', None)))

connection.commit()

cursor.close()
connection.close()

print("Dados inseridos no banco")