import pandas as pd
import os
import logging
import numpy as np
import warnings
from pandas.errors import DtypeWarning
from pandas.errors import ParserWarning
import sys
import gc

from banco import *
from gerais import *
from csv_config import V2DataSet, DescProdDataSet, ProjDataSet
from bq import consulta_bq, consulta_bq_nets, consulta_bq_maga

os.environ['TMPDIR'] = 'D:\Tmpdf'
os.environ['TMP'] = 'D:\\Tmpdf'
os.environ['TEMP'] = 'D:\\Tmpdf'

#=======================================================================================#
# MÉTODO PARA LEITURA DOS ARQUIVOS CSV 
#=======================================================================================#
def read_and_combine_files(dataset, post_process_func=None):
    if not dataset.caminho or not dataset.arquivos or not isinstance(dataset.read_args, dict):
        logging.error("Invalid arguments provided.")
        return None
    
    df_list = []
    
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DtypeWarning)
            warnings.filterwarnings("ignore", category=ParserWarning)

            for file in dataset.arquivos:
                file_path = os.path.join(dataset.caminho, file)
                df_temp = pd.read_csv(file_path, **dataset.read_args)
                if post_process_func:
                    df_temp = post_process_func(df_temp)
                df_list.append(df_temp)
            return pd.concat(df_list, ignore_index=True)
        
    except FileNotFoundError as e:
        logging.error(f"Falta um arquivo da lista: {e}")
        return None
    except pd.errors.EmptyDataError as e:
        logging.error(f"Arquivo vazio: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro: {e}")
        return None

#=======================================================================================#
# MÉTODO PARA CONSULTA NO BANCO DE DADOS
#=======================================================================================#

def lista_banco(soli_str, cursor):

    query = f"""
        SELECT Pedido,Numero_Entrega_Documento, Valor_Mercadoria
        FROM Pre_Fat_CTE
        WHERE Pedido IN ({soli_str})
    """
    cursor.execute(query)
    results = cursor.fetchall()

    return results



#=======================================================================================#
# MÉTODO PARA CONVERSÃO DAS COLUNAS PARA FLOAT * SOMENTE NA LEITURA DOS ARQUIVOS
#=======================================================================================#

def post_process(df):
    # Modificação para trocar vírgula por ponto e converter para decimal
    for col in df.columns[9:25]:
        df[col] = df[col].str.replace(',', '.').astype(float)
    
    return df

#=======================================================================================#
# PARA GERAÇÃO DOS ARQUIVOS PARA PESQUISA NO SINCLOG E FINALIZA O PROCESSO
#=======================================================================================#

def Entrega_50 (df_v2):
    chunk_size = 50000
    for i in range(0, len(df_v2['Nro. Entrega']), chunk_size):
        df_slice = df_v2['Nro. Entrega'].iloc[i:i+chunk_size]
        df_slice.to_csv(f'D:\\PRojetos\\Bases\\Reajuste_Frete\\txt_descr\\dataframe_part_{i//chunk_size + 1}.txt', sep='\t', index=False)
    #sys.exit()

#=======================================================================================#
# MÉTODO PARA CONVERSÃO DAS COLUNAS PARA FLOAT
#=======================================================================================#

def convert_to_float(s):
    return float(s.replace(',', '.'))

def convert_float_columns(df):
    for column in df.columns:
        if df[column].dtype == 'float64':
            df[column] = df[column].apply(lambda x: str(x).replace('.', ','))
    return df

#=======================================================================================#
# ESSES DOIS FAZEM O MESMO PROCESSO A DIFEREÇA É QUE UM ESCREVE NA COLUNA E O OUTRO O VALOR
# Separar apenas 1 sku: coluna peso informado / coluna peso do cadastro
# Filtrar apenas os números inteiros (Os fracionados possuem mais de 1 sku, então devemos desconsiderar)
#=======================================================================================#

def calculate_ratio_gueilee(row):
    if row['peso'] == 0:  # Verifica se o divisor é zero
        return "Indefinido"  
    ratio = row['Peso Informado'] / row['peso']
    if ratio.is_integer():  # Verifica se o resultado da divisão é um número inteiro
        return "Inteiro"
    else:
        return "Fracionado"

def calculate_ratio(row):
    if row['peso'] == 0: 
        return 0
    elif row['Peso Informado'] % row['peso'] == 0:
        return row['Peso Informado'] / row['peso']
    else:
        return row['Peso Informado'] / row['peso']

#=======================================================================================#
# FUNCAO PARA CALCULAR A MODA E MEDIANA
# Desconsiderar essa funcao (nao mais ultilizada)
#=======================================================================================#

def custom_mode(s):
    # Obtenha a contagem de valores
    counts = s.value_counts()
    # Se o maior valor de contagem for 1, retorne "Moda indisponível"
    if counts.iloc[0] == 1:
        return 'Moda indisponível'

    return counts.index[0]

#==========================================================================================

def semi_iqr(data):
    q1 = np.percentile(data, 25)
    q3 = np.percentile(data, 75)
    semi_iqr_value = (q3 - q1) / 2
    return semi_iqr_value

#=======================================================================================#
# ESSA FUNÇÃO FARA A VERIFICAÇÃO SE O VALOR DA MEDIANA É MAIOR QUE O PESO * 1.3(+30%)
# OU MENOR QUE O PESO * 0.7(-30%)
#=======================================================================================#

def analise_30_percent(row):
    upper_threshold = row['Mediana'] * 1.3
    lower_threshold = row['Mediana'] * 0.7  
    
    if row['peso'] > upper_threshold:
        return 'maior'
    elif row['peso'] < lower_threshold:
        return 'menor'
    else:
        return 'entre'
    
#=======================================================================================#
# CLASSIFICAÇÃO DE VALORES DE ACORDO COM A FAIXA
#=======================================================================================#

def classify_value(x,ranges):
    for low, high, label in ranges:
        if low <= x <= high:
            return label
    
    return "Acima de 30"

def main():

    #=======================================================================================#
    # LEITURA V2
    #=======================================================================================#
    print('Iniciando V2.')
    
    v2_data = V2DataSet(cam_v2)
    df_v2 = read_and_combine_files(v2_data)

    df_v2 = df_v2.loc[(df_v2['Peso Medido'] != '0,000') & (df_v2['Peso Medido'].notna())]
    df_v2['Nro. Pedido'] = df_v2['Nro. Pedido'].str.replace('="', '').str.replace('"', '')
    #df_v2 = df_v2.loc[(df_v2['Peso Medido'] != '0,000')]

    # SELECIONANDO SOMENTE COLUNAS NECESSÁRIAS
    df_v2 = df_v2[['Nro. Entrega','Nro. Pedido','Cliente','Filial','Tipo Serviço','Dt. Cadastro','CEP Pessoa Visita','UF Pessoa Visita','Código Remetente']]

    # CHAMANDO A FUNÇÃO PARA GERAR OS ARQUIVOS PARA PESQUISA NO SINCLOG (SE ATIVADA O PROCESSO É FINALIZADO)
    #Entrega_50(df_v2)

    #=======================================================================================#
    # LEITURA DOS ARQUIVOS DE DESCRIÇÃO, BAIXADOS A PARTIR DOS ARQUIVOS GERADO PELA FUNCAO ENTREGA_50
    #=======================================================================================#

    print('Iniciando Descricao.')
    desc1_data = DescProdDataSet(cam_desc)
    df_desc1 = read_and_combine_files(desc1_data)
    print('Descricao finalizado.', df_desc1.count())
    # CONTANDO QUANTOS ITENS TEM EM CADA PEDIDO E DEIXANDO SOMENTE OS QUE TEM 1 ITEM
    df_desc1['solicitacao_count'] = df_desc1.groupby('Solicitação')['Solicitação'].transform('count')
    df_desc = df_desc1[df_desc1['solicitacao_count'] == 1]

    #=======================================================================================#
    # MERGE V2 MAIS DESCRIÇÃO
    #=======================================================================================#

    df_v22 = df_desc.merge(df_v2,left_on=['Solicitação','Nro. Pedido'], right_on=['Nro. Entrega','Nro. Pedido'], how='right')

    del df_v2,df_desc1 
    gc.collect()
    #=======================================================================================#
    # LEITURA BQ
    #=======================================================================================#
    # ADD Consulta BQ 07.11.2022

    print('Iniciando BQ.')
    #bq_data = BQDataSet(cam_bq)
    #df_bq = read_and_combine_files(bq_data)
    df_bq = consulta_bq()
    print('BQ finalizado.',df_bq.count())

    #=======================================================================================#
    # JUNCAO BQ + V2
    #=======================================================================================#

    df_bqv2 = df_bq.merge(df_v22, left_on=['pedido','seller'],right_on=['Nro. Pedido','Código Remetente'],how='inner')

    #=======================================================================================#
    # Processo DE LEITURA projecao de frete
    #=======================================================================================#

    print('Iniciando Projecao.')
    proj_data = ProjDataSet(cam_proj)
    df_proj = read_and_combine_files(proj_data)
    print('Projecao finalizado.')

    #=======================================================================================#
    # PEGANDO SOMENTE ONDE O SERVIÇO ESTA LISTADO NA LISTA 3P, SOMENTE O QUE O PESO AFERIDO
    # É 7 DIFERENTE DE 0
    # ETAPA: (V2) Na coluna AX (Peso Medido) tudo que for zero deve excluir
    #=======================================================================================#

    df_proj = df_proj[df_proj['Servico'].isin(list_3p)]

    #=======================================================================================#
    # (proj) Na coluna "U" Peso aferido, oq for = 0 excluir
    #=======================================================================================#

    df_proj = df_proj.loc[(df_proj['Peso Aferido'] != '0,000')]
    df_proj['Aferido > informado'] = np.where(df_proj['Peso Aferido'] > df_proj['Peso Informado'], 'Maior', 'Menor')

    #=======================================================================================#
    # MERGE COM A BASE BQ+V2(df_bqv2) COM A TABELA PROJEÇÃO
    # *** Abrir o Projeção de Frete consolidado
    #=======================================================================================#
    # (proj) Fazer um procv pelo número da remessa buscando no arquivo BQ o SKU
    #=======================================================================================#

    df_bqv2proj = df_bqv2.merge(df_proj, left_on='Nro. Entrega', right_on='Nro. Remessa', how='inner', suffixes=('', '_proj'))

    #=======================================================================================#
    # SOMENTE ANALISE
    #=======================================================================================#

    print('PROJ',df_proj.shape)
    print('Merge',df_bqv2.shape)
    print('teste',df_bqv2proj.shape)

    #=======================================================================================#
    # CONTAGEM DE SKU
    # (proj) Fazer um cont.se para trazer a quantidade de passagens de cada SKU pelo sorter
    #=======================================================================================#

    df_contagem = df_bqv2proj['sku'].value_counts()
    df_bqv2projcont = df_bqv2proj.merge(df_contagem, on='sku', how = 'inner')

    #=======================================================================================#
    # APENAS LIMPEZA
    #=======================================================================================#
    cols_to_drop = [col for col in df_bqv2projcont.columns if '_proj' in col]
    df_bqv2projcont.drop(columns=cols_to_drop, inplace=True)

    #=======================================================================================#
    # listagem dos SKUS para busca no bq, contendo informações de cadastro
    #=======================================================================================#

    df_listesku = df_bqv2projcont['sku']
    df_listesku.drop_duplicates(inplace=True)
    df_listesku.to_csv(f'D:\\PRojetos\\Bases\\Reajuste_Frete\\txt_descr\\skus23.csv', sep=';', index=False)

    #=======================================================================================#
    # Processo leitura sku_bq
    # após baixar os dados no bq da lista a cima
    # atualizacao para automacao bq
    #=======================================================================================#

    #maga_data = MagaDataSet(cam_magalu)
    #df_skubqmagalu = read_and_combine_files(maga_data)
    # atualizado lista 13/11/2023
    df_skumagalu = df_listesku[~df_listesku.str.contains('-')]

    list_skumagalu = df_skumagalu.tolist()
    print('Consulta BQ SKU Magalu.')
    df_skubqmagalu = consulta_bq_maga (list_skumagalu)
    
    #=======================================================================================#
    # somente NETSHOES por conta dos valores em CM, G. *** falta realizar ***
    # *** fazer a conversão ***
    # 
    #=======================================================================================#
    # Alteração para automacao das consultas no bq, que demandam bastante tempo
    #nets_data = NetsDataSet(cam_nets)
    #df_skubqnets = read_and_combine_files(nets_data)

    df_skunets = df_listesku[df_listesku.str.contains('-')]
    list_skunets = df_skunets.tolist()
    print('Consulta BQ SKU Nets.')
    df_skubqnets = consulta_bq_nets(list_skunets)

    # LIMPANDO DESCRICAO POIS VEM BASTANTE EXPRESSAO REGULAR
    df_skubqnets['descricao'] = df_skubqnets['descricao'].str.replace('\r', '', regex=True)

    df_skubqnets['peso'] = df_skubqnets['peso'] / 1000  # convertendo peso para quilogramas

    # Converter dimensões de centímetros para metros (NA TABELA NETSHOES APARENTA TER SIDO 
    # CADASTRADO EM CENTÍMETROS MAS ALGUNS ESTA EM METROS)

    df_skubqnets['altura'] = df_skubqnets['altura'] / 100 
    df_skubqnets['largura'] = df_skubqnets['largura'] / 100
    df_skubqnets['profundidade'] = df_skubqnets['profundidade'] / 100

    #=======================================================================================#
    # CONCATENANDO BANCO DE CADASTRO MAGALU E NETSHOES
    #=======================================================================================#
    print('Concatenando bases SKUs MAGANETS')
    df_skumaganets = pd.concat([df_skubqnets, df_skubqmagalu], ignore_index=True)

    del df_skubqmagalu,df_skubqnets
    gc.collect()

    #=======================================================================================#
    # df_bqv2projcont é o é o bq + v2 (cont) + info cadastro
    #=======================================================================================#

    df_projebq = df_bqv2projcont.merge(df_skumaganets, on='sku', how = 'inner')
    del df_skumaganets,df_bqv2projcont,df_bqv2
    gc.collect()
    #=======================================================================================#
    # Aplicar a função nas colunas específicas do df_projebq
    #=======================================================================================#

    df_projebq['Peso Aferido'] = df_projebq['Peso Aferido'].apply(convert_to_float)
    df_projebq['Peso Informado'] = df_projebq['Peso Informado'].apply(convert_to_float)

    # VERIFICAND SE O PESO MEDIDO É 100% MAIOR QUE O PESO INFORMADO
    df_projebq['Aferido 100% < Informado'] = df_projebq.apply(lambda row: "Acima de 100%" if row['Peso Aferido'] > 2 * row['Peso Informado'] else "Menor ou igual a 100%", axis=1)

    # MEDIDA DE TOLERANCIA(NECESSÁRIO PARA COMPARACAO DE FLOATS NO PYTHON)
    tolerancia = 1e-5  # Uma pequena margem de erro, como 0.00001

    df_projebq['Aferido <> 0,100'] = df_projebq.apply(lambda row: "Igual" if abs(row['Peso Aferido'] - 0.10) < tolerancia else "Diferente", axis=1)

    df_projebq['Nº Inteiro'] = df_projebq.apply(calculate_ratio_gueilee, axis=1)

    df_projebq['Apenas 1 sku'] = df_projebq.apply(calculate_ratio, axis=1)

    #=======================================================================================#
    #df_projbq2['Peso unitário'] = df_projbq2['Peso Aferido'] / df_projbq2['Apenas 1 sku']
    # CRIA A COLUNA PESO UNITARIO
    #=======================================================================================#

    print('Peso unitario.')
    df_projebq['Peso unitário'] = np.where(
        df_projebq['Apenas 1 sku'] != 0, #EVITANDO DIVISÃO POR 0
        df_projebq['Peso Aferido'] / df_projebq['Apenas 1 sku'],
        0
    )

    #=======================================================================================#
    # Tudo que for aferido menor que o informado excluir
    # - [x]  Tudo que for aferido 100% acima do informado excluir
    # formula: =SE(AC3>(AB3*2);"Acima de 100%";"Menor")
    #=======================================================================================#

    df_projbq2 = df_projebq[(df_projebq['Peso Informado'] <= df_projebq['Peso Aferido'])]
    df_projbq3 = df_projbq2[df_projbq2['Peso Informado'] *2 < df_projbq2['Peso Aferido']]

    #=======================================================================================#
    # Eliminar medições = 0,100
    #=======================================================================================#

    #df_projbq4 = df_projbq3[df_projbq3['Peso Aferido'] != 0.100]
    df_projbq4 = df_projbq3[abs(df_projbq3['Peso Aferido'] - 0.10) > tolerancia]
    # Criando a coluna 'Apenas 1 sku'
    df_projbq4['Apenas 1 sku'] = df_projbq4.apply(calculate_ratio, axis=1)

    #=======================================================================================#
    #infinito
    #df_projbq2['Peso unitário'] = df_projbq2['Peso Aferido'] / df_projbq2['Apenas 1 sku']
    # Construir a coluna do peso unitário: Dividir a coluna peso aferido pela coluna da etapa interior (Apenas 1 SKU)
    #=======================================================================================#

    # Cria a coluna 'Peso unitário'
    df_projbq4['Peso unitário'] = np.where(
        df_projbq4['Apenas 1 sku'] != 0,
        df_projbq4['Peso Aferido'] / df_projbq4['Apenas 1 sku'],
        0
    )

    #=======================================================================================#
    # Excluir os números de pedidos que se repetem
    #=======================================================================================#
    # Alterando para teste ******* correto é o comentado
    #df_projbq4 = df_projbq4[df_projbq2['pedido'].duplicated(keep=False) == False]
    df_projebq = df_projebq[df_projebq['pedido'].duplicated(keep=False) == False]


    #=======================================================================================#
    # Aplicando o calculo na base
    #=======================================================================================#
    print('Calculo mediana e semi_iqr.')
    agg_funcs = {
        'Peso unitário': ['median', semi_iqr]
    }
    #Correto é o comentado alteração para teste
    #result = df_projbq4.groupby('sku').agg(agg_funcs)
    result = df_projebq.groupby('sku').agg(agg_funcs)
    result.columns = ['Mediana','Semi_IQR']
    #=======================================================================================#
    # LEITURA DO BANCO DE DADOS A PARTIR DA LISTA DE PEDIDOS DO PROJECAO DE FRETE
    # RETORNA O VALOR DA NOTA E ENTREGA DOCUMENTO
    #=======================================================================================#
    print('Consulta DW_GFL.')
    with DatabaseConnection() as cursor:
        soli_list = df_projebq['Nro. Pedido']
        soli_list.count()
        soli_str = ', '.join([f"'{i}'" for i in soli_list])
        soli_list.drop_duplicates(inplace=True)
        results2 = lista_banco(soli_str, cursor)

    df_endereco = pd.DataFrame(results2, columns=['Pedido','Numero_Entrega_Documento', 'Valor_Mercadoria'])
    df_endereco.drop_duplicates('Pedido',inplace = True)

    #==============================================s=========================================#
    # MODA E MEDIANA NO DATARAME PRINCIPAL
    #========================================================================================#

    #correto é o comentado, teste
    #df_projbq4 = df_projbq4.merge(result, on='sku', how='left')
    df_projebq = df_projebq.merge(result, on='sku', how='left')

    #=======================================================================================#
    # ENDERECO E DEMAIS INFO NO DATAFRAME PRINCIPAL
    #=======================================================================================#

    ##df_projbq4 = df_projbq4.merge(df_endereco, left_on='Nro. Pedido',right_on='Pedido', how='left')

    df_projebq = df_projebq.merge(df_endereco, left_on='Nro. Pedido',right_on='Pedido', how='left')

    #=======================================================================================#
    # Mediano x Cadastro (coluna peso mediano - coluna peso do cadastro)
    #=======================================================================================#
     # ## igual a correto é o comentado 
    ##df_projbq4['Mediana X Cadastro'] = df_projbq4['Mediana'] - df_projbq4['peso']

    df_projebq['Mediana X Cadastro'] = df_projebq['Mediana'] - df_projebq['peso']

    #=======================================================================================#
    # Qtd SKU x Peso Mediano (coluna Apenas 1 SKU x coluna peso mediano)
    #=======================================================================================#

    ##df_projbq4['Qtd SKU x Peso Mediano'] = df_projbq4['Apenas 1 sku'] * df_projbq4['Mediana']
    df_projebq['Qtd SKU x Peso Mediano'] = df_projebq['Apenas 1 sku'] * df_projebq['Mediana']

    #=======================================================================================#
    # Analise 30%
    #=======================================================================================#

    #df_projbq4['Analise 30%'] = df_projbq4.apply(lambda row: 'Abaixo' if row['Mediana'] < row['peso'] * 1.3 else ('Acima' if row['Mediana'] > row['peso'] * 1.3 else 'Entre'), axis=1)
    #df_projbq4['Analise 30%'] = df_projbq4.apply(analise_30_percent, axis=1)
    df_projebq['Analise 30%'] = df_projebq.apply(analise_30_percent, axis=1)
    #df_projbq4[['Analise 30%','Mediana','peso']].to_csv(r'C:\Users\PedroFerreiraMouraBa\Desktop\Nova pasta (7).csv', sep=';')
    #colunas_df2 = ['SKU', 'Preço']


    df_projebq['Peso taxado Atual'] = df_projebq['Peso taxado Atual'].apply(convert_to_float)
    #df_projebq['Peso taxado Recalculado SKU'] = df_projebq['Qtd SKU x Peso Mediano'].apply(convert_to_float)
    #df_projebq['Class Peso taxado atual'] = df_projebq['Peso taxado Atual'].apply(classify_value(ranges))
    df_projebq['Class Peso taxado atual'] = df_projebq['Peso taxado Atual'].apply(lambda x: classify_value(x, ranges))
    df_projebq['Class Peso taxado recalculado'] = df_projebq['Qtd SKU x Peso Mediano'].apply(lambda x: classify_value(x, ranges))
    #df_projebq['Class Peso taxado recalculado'] = df_projebq['Qtd SKU x Peso Mediano'].apply(classify_value(ranges))

    df_projebq['Houve mudança de faixa?'] = df_projebq.apply(lambda row: "Não" if abs(row['Class Peso taxado atual'] == row['Class Peso taxado recalculado']) else "Sim", axis=1)

    #=======================================================================================#
    # GERACAO DO ARQUIVO FINAL PARA ENVIO RONALDO
    #=======================================================================================#
    df_projebq = convert_float_columns(df_projebq)
    df_projebq['Nro. Pedido'] = '="' + df_projebq['Nro. Pedido'].astype(str) + '"'
    print('Gerando CSV.')
    df_projebq[['Nro. Remessa','Nro. Pedido','sku','count','Cliente','Filial','Tipo Serviço','Dt. Cadastro','Dt. Realização','Dt. Emissão CTe','Cod. Remetente',
                'Valor Frete Peso Atual','Valor Frete Peso Recalculado','Gris Atual','Gris Recalculado', 'AdValores Atual', 'AdValores Recalculado',
                'ICMS Atual', 'ICMS Recalculado','Valor Total Atual','Valor Total Recalculado','descricao','altura', 'largura', 'profundidade', 'peso',
                'Peso Informado', 'Peso Aferido','Aferido > informado','Aferido 100% < Informado','Aferido <> 0,100','Apenas 1 sku','Nº Inteiro','Peso unitário',
                'Mediana','Semi_IQR','Mediana X Cadastro','Qtd SKU x Peso Mediano', 'Analise 30%','Cubagem Informada', 'Cubagem Aferida',
                'Peso taxado Atual','Class Peso taxado atual','Class Peso taxado recalculado','Houve mudança de faixa?','Numero_Entrega_Documento', 'Valor_Mercadoria','CEP Pessoa Visita',
                #'Peso taxado Atual','Class Peso taxado atual','Class Peso taxado recalculado','Houve mudança de faixa?','CEP Pessoa Visita',
                'UF Pessoa Visita']].to_csv(cam_fin,sep=';',encoding='utf-8', index=False)


    #for col in df_projebq.columns:
    #    for value in df_projebq[col].unique():
    #        if isinstance(value, str):
    #            if '\u"""2022' in value:
    #                print(f'Caractere problemático encontrado na coluna {col}: {value}')

    # Substituir o caractere de bullet point por uma string vazia ('') em todas as colunas de descrição
    #for col in df_projebq.columns:
    #    if 'descricao' in col.lower():  # Verificar se a coluna contém "descricao"
    #        df_projebq[col] = df_projebq[col].str.replace('\u25fe', '', regex=False)


if __name__ == "__main__":
    main()


