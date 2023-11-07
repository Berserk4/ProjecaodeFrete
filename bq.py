import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

load_dotenv(r'D:\\PRojetos\\Codes\\GFL\\Financeiro\\NewReajusteFrete\\querys\bq.env')
prj_id = os.getenv('project-id')

def consulta_bq ():
    with open(r'D:\\PRojetos\\Codes\\GFL\\Financeiro\\NewReajusteFrete\\querys\\geral.txt', 'r') as file:  # ajuste o caminho do arquivo conforme necessário
        query = file.read()
    data_atual = datetime.today()

    data_bq = data_atual - relativedelta(months=1)
    data_bq = data_bq.replace(day=24)
    dia_24 = data_bq.strftime('%Y-%m-%d')
    query = query.format(data=dia_24)
    df = pd.read_gbq(query,project_id = prj_id)
    return df

def consulta_bq_maga(list):
    chunk_size = 50000
    sku_chunks = [list[i:i + chunk_size] for i in range(0, len(list), chunk_size)]
    df_final = pd.DataFrame()
    with open(r'D:\\PRojetos\\Codes\\GFL\\Financeiro\\NewReajusteFrete\\querys\\maga.txt', 'r') as file:  
        query_template = file.read()
    for skus in sku_chunks:
        # Criar uma string com SKUs formatados para a cláusula SQL IN
        formatted_skus = "','".join(skus)
        # Substituir o placeholder na consulta pelo conjunto formatado de SKUs
        query = query_template.format(formatted_skus=formatted_skus)
    
        # Executar a consulta e armazenar o resultado em um DataFrame temporário
        df_temp = pd.read_gbq(query,project_id = prj_id)
    
        # Concatenar o resultado com o DataFrame final
        df_final = pd.concat([df_final, df_temp], ignore_index=True)

    return df_final

def consulta_bq_nets(list):
    chunk_size = 50000
    sku_chunks = [list[i:i + chunk_size] for i in range(0, len(list), chunk_size)]
    df_final = pd.DataFrame()

    with open(r'D:\\PRojetos\\Codes\\GFL\\Financeiro\\NewReajusteFrete\\querys\\nets.txt', 'r') as file:  
        query_template = file.read()

    # Executar a consulta para cada subconjunto de SKUs
    for skus in sku_chunks:
        # Criar uma string com SKUs formatados para a cláusula SQL IN
        formatted_skus = "','".join(skus)
        # Substituir o placeholder na consulta pelo conjunto formatado de SKUs
        query = query_template.format(formatted_skus=formatted_skus)
    
        # Executar a consulta e armazenar o resultado em um DataFrame temporário
        df_temp = pd.read_gbq(query,project_id = prj_id)
    
        # Concatenar o resultado com o DataFrame final
        df_final = pd.concat([df_final, df_temp], ignore_index=True)

    return df_final

