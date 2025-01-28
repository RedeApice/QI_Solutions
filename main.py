import requests

chave_api = "QO8RUN8JR24D63t5PtR9kEQ794k88560G954IR53E5v336RQH8"
ano_letivo = "2024"

url = f"https://apiqis.educacionalcloud.com.br/api/alunos/{chave_api}?ano_letivo={ano_letivo}"

headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {chave_api}"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    print("Resposta da API:", response.json())
else:
    print(f"Erro na requisição: {response.status_code}")
    print("Detalhes:", response.text)
