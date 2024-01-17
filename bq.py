import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

load_dotenv(r'D:\\PRojetos\\Codes\\GFL\\Financeiro\\NewReajusteFrete\\querys\bq.env')
prj_id = os.getenv('project-id')
tbl_3p = os.getenv('3p_processado_tbl')
tbl_maga = os.getenv('maga_tbl')
tbl_nets = os.getenv('nets_tbl')


def consulta_bq ():

    with open(r'D:\\PRojetos\\Codes\\GFL\\Financeiro\\NewReajusteFrete\\querys\\geral.txt', 'r') as file:  
        query = file.read()
        
    data_atual = datetime.today()
    data_bq = data_atual - relativedelta(months=2)
    data_bq = data_bq.replace(day=24)
    dia_24 = data_bq.strftime('%Y-%m-%d')

    query = query.format(data=dia_24, t3p_processado_tbl=tbl_3p)

    
    df = pd.read_gbq(query,project_id = prj_id)

    return df

def consulta_bq_maga(list):

    chunk_size = 50000
    sku_chunks = [list[i:i + chunk_size] for i in range(0, len(list), chunk_size)]

    df_final = pd.DataFrame()

    with open(r'D:\\PRojetos\\Codes\\GFL\\Financeiro\\NewReajusteFrete\\querys\\maga.txt', 'r') as file:  
        query_template = file.read()
    query_template = query.format(maga=tbl_maga)

    for skus in sku_chunks:
        formatted_skus = "','".join(skus)
        query = query_template.format(formatted_skus=formatted_skus)
        df_temp = pd.read_gbq(query,project_id = prj_id)
        df_final = pd.concat([df_final, df_temp], ignore_index=True)

    return df_final

def consulta_bq_nets(list):

    chunk_size = 50000
    sku_chunks = [list[i:i + chunk_size] for i in range(0, len(list), chunk_size)]
    
    df_final = pd.DataFrame()

    with open(r'D:\\PRojetos\\Codes\\GFL\\Financeiro\\NewReajusteFrete\\querys\\nets.txt', 'r') as file:  
        query_template = file.read()
    query_template = query.format(nets=tbl_nets)

    for skus in sku_chunks:
        formatted_skus = "','".join(skus)
        query = query_template.format(formatted_skus=formatted_skus)
        df_temp = pd.read_gbq(query,project_id = prj_id)
        df_final = pd.concat([df_final, df_temp], ignore_index=True)

    return df_final

