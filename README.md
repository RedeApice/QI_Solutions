# QI Solutions - Integração de Dados com API, Pandas e MySQL

## Visão Geral

Este projeto Python integra funcionalidades para importar dados de uma API externa, processar esses dados utilizando Pandas e, finalmente, armazenar os dados em um banco de dados MySQL. O script segue uma abordagem modular para carregar configurações do ambiente, estabelecer conexão com o banco de dados, criar tabelas (caso não tenha), manipular dados e inserir registros filtrados no banco de dados.

## Estrutura do Código

### Importações

As bibliotecas usadas são fundamentais para a execução das tarefas de manipulação de dados, conexão com banco de dados e requisições HTTP:

```python
import pandas as pd
import requests
import pymysql
from dotenv import load_dotenv
import os