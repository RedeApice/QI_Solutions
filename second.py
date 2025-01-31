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
    ano_competencia INT(4),
    mes_competencia VARCHAR(2),
    cod_tipocarne VARCHAR(3),
    nome_tipocarne VARCHAR(19),
    cod_parcela INT(10),
    nome_aluno VARCHAR(48),
    cod_curso VARCHAR(3),
    curso VARCHAR(21),
    valor_bruto VARCHAR(7),
    valor_bolsa VARCHAR(7),
    valor_desconto VARCHAR(5),
    dt_vencimento VARCHAR(19),
    dt_desconto VARCHAR(19),
    cod_turma VARCHAR(7),
    turma VARCHAR(29),
    situacao INT(1),
    valor_pago VARCHAR(10),
    dt_pagamento VARCHAR(10),
    dt_credito VARCHAR(19),
    nome_usuario INT(5),
    responsavel VARCHAR(48),
    linhaDigitavel VARCHAR(59),
    nossoNumero VARCHAR(8),
    banco VARCHAR(10),
    cdbnc VARCHAR(10),
    agencia VARCHAR(10),
    conta VARCHAR(10),
    tipoOperacao VARCHAR(10),
    acrescimo VARCHAR(10),
    menor VARCHAR(10),
    observacao VARCHAR(10),
    valor_Taxa VARCHAR(10),
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
INSERT INTO {tbl_name} (ano_competencia, mes_competencia, cod_tipocarne, nome_tipocarne, cod_parcela, nome_aluno,
cod_curso, curso, valor_bruto, valor_bolsa, valor_desconto, dt_vencimento, dt_desconto, cod_turma, turma, situacao, valor_pago,
dt_pagamento, dt_credito, nome_usuario, linhaDigitavel, nossoNumero, banco, cdbnc, agencia, conta, tipoOperacao, acrescimo, menor,
observacao, valor_Taxa) 
VALUES (%s, %s, %s)
'''

for _, row in df_filtrado.iterrows():
    cursor.execute(sql, (row.get('data_pagamento'), row['valor_pago'], row.get('observacoes', None)))

connection.commit()

cursor.close()
connection.close()

print("Dados inseridos no banco")