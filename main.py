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

# Conectar ao Banco de Dados
try:
    connection = pymysql.connect(host=host, user=usuario, password=senha, db=nome_banco)
    cursor = connection.cursor()

    # Criar tabela se não existir
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
        dt_pagamento VARCHAR(19),
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
        nome_escola VARCHAR(25)
    )
    ''')

    # Request na API do QI
    headers = {
        'Authorization': Auth,
        'Accept': 'application/json'
    }
    response = requests.get(endereco, headers=headers)
    response.raise_for_status()
    data = response.json()

    # Processar os dados
    for item in data:
        if 'nome_tipocarne' in item:
            nome_tipocarne = item['nome_tipocarne'].lower().replace(' ', '').replace('-', '')
            if 'km18' in nome_tipocarne:
                item['nome_escola'] = 'Dom Henrique II'
            else:
                item['nome_escola'] = 'Dom Henrique I'

    # Filtrar para pegar só os que estão com "valor_pago" == NULL
    df = pd.DataFrame(data)
    df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce')
    df_filtrado = df[df['valor_pago'].isna()]

    dados_para_inserir = []
    for _, row in df_filtrado.iterrows():
        valores = [
            None if pd.isna(valor) else valor
            for valor in [
                row.get('ano_competencia'), row.get('mes_competencia'), row.get('cod_tipocarne'),
                row.get('nome_tipocarne'), row.get('cod_parcela'), row.get('nome_aluno'),
                row.get('cod_curso'), row.get('curso'), row.get('valor_bruto'), row.get('valor_bolsa'),
                row.get('valor_desconto'), row.get('dt_vencimento'), row.get('dt_desconto'),
                row.get('cod_turma'), row.get('turma'), row.get('situacao'), row.get('valor_pago'),
                row.get('dt_pagamento'), row.get('dt_credito'), row.get('nome_usuario'), row.get('responsavel'),
                row.get('linhaDigitavel'), row.get('nossoNumero'), row.get('banco'), row.get('cdbnc'),
                row.get('agencia'), row.get('conta'), row.get('tipoOperacao'), row.get('acrescimo'),
                row.get('menor'), row.get('observacao'), row.get('valor_Taxa'), row.get('nome_escola')
            ]
        ]
        dados_para_inserir.append(valores)

    # Inserir os dados no banco
    sql = f'''
    INSERT INTO {tbl_name} (
        ano_competencia, mes_competencia, cod_tipocarne, nome_tipocarne, cod_parcela, nome_aluno,
        cod_curso, curso, valor_bruto, valor_bolsa, valor_desconto, dt_vencimento, dt_desconto, 
        cod_turma, turma, situacao, valor_pago, dt_pagamento, dt_credito, nome_usuario, responsavel, 
        linhaDigitavel, nossoNumero, banco, cdbnc, agencia, conta, tipoOperacao, acrescimo, menor, 
        observacao, valor_Taxa, nome_escola
    ) 
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    '''
    cursor.executemany(sql, dados_para_inserir)
    connection.commit()

    cursor.execute(f"SELECT * FROM {tbl_name}")
    resultados = cursor.fetchall()

    print("Dados na tabela após o insert:")
    for linha in resultados:
        print(linha)

except Exception as e:
    print(f"Erro durante a execução: {e}")
finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    print("Fechou.")

print("Dados Inseridos.")