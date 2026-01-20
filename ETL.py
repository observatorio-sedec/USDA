import datetime
import requests as rq
import pandas as pd
from pathlib import Path
from dicionarios import country_dict, commodity_dict, unidades_descricoes, name_attribute
# from ajustar_planilha import ajustar_bordas, ajustar_colunas
ROOT_PATH = Path(__file__).parent

lista_paises = ['AF','AL','AG','AO','AC','AR','AM','AS','AU','AJ','BF','BA','BG','BB','BO','S8','BE','BH','DM','BD','BT','BL','BK','BC','BR','BX','BU','UV','BM',
                'BY','CV','CB','CM','CA','CT','CD','CI','CH','CO','CN','CF','CG','CS','IV','HR','CU','CY','EZ','DA','DJ','DO','DR','EC','EG','ES','EK','ER','EN','WZ',
                'ET','E2','E3','E4','FA','FO','FJ','FI','CZ','YO','FT','FR','FG','FP','Y2','GB','GA','GZ','GG','GC','GM','GE','GH','GI','GN','GR','GL','GJ','GP','GT',
                'GU','PU','GY','HA','HO','HK','HU','IC','IN','ID','IR','IZ','EI','IS','IT','JM','JA','JO','KZ','KE','KN','KS','KV','KU','KG','LA','LG','LE','LT','LI',
                'LY','LH','LU','MC','MA','MI','MY','MV','ML','MT','MB','MR','MP','MX','MD','MG','MJ','MO','MZ','WA','NP','NL','NA','NC','NZ','NU','NG','NI','MK','NO',
                'MU','ZZ','PK','PN','PP','PA','PE','RP','PL','PO','RQ','QA','RE','RO','RS','RW','WS','TP','SA','SG','RB','SR','SE','SL','SN','LO','SI','BP','SO','SF',
                'OD','SP','CE','SC','ST','VC','SU','NS','SW','SZ','SY','TW','TI','TZ','TH','TO','TN','TD','TS','TU','TX','UG','UP','UR','TC','UK','US','UY','UZ','NH',
                'VE','VM','VO','YM','YS','YE','YU','ZA','RH'
]

lista_mercadoria = ['0577400', '0011000', '0013000', '0574000', '0430000', '0579305', '0711100', '0440000', '2631000', '0230000', '0240000', '0224400', '0223000', '0224200', '0572220', '0575100', 
                    '0572120', '0813700', '0813300', '0814200', '0813800', '0813200', '0813600', '0813100', '0813101', '0813500', '0111000', '0115000', '0113000', '0459100', '0459900', '0452000', 
                    '4242000', '4233000', '4235000', '4243000', '4244000', '4234000', '4239100', '4232000', '4232001', '4236000', '2231000', '2223000', '2232000', '2221000', '2226000', '2222000', 
                    '2222001', '2224000', '0585100', '0571120', '0579309', '0579220', '0577907', '0114200', '0422110', '0451000', '0459200', '0612000', '0571220', '0577901', '0410000', ]

ano_atual = datetime.datetime.now().year
ano_inicial = ano_atual - 5  
lista_periodo = [str(ano) for ano in range(ano_inicial, ano_atual + 1)]

def tratando_dados_producao(dados_brutos):
    dados_limpos = []

    for ii in dados_brutos:
        codigo_mercadoria = ii['commodityCode']
        pais = ii['countryCode']
        ano_mercado = ii['marketYear']
        ano_lancamento = ii['releaseYear']
        mes = ii['releaseMonth']
        
        dict_valor = {
            'codigo_mercadoria': codigo_mercadoria,
            'pais': pais,
            'ano_mercado': ano_mercado,
            'ano_lancamento': ano_lancamento,
            'mes': mes,
        }
        dados_limpos.append(dict_valor)
        
    return dados_limpos

def tratando_dados_forn(dados_brutos):
    dados_limpos = []

    for ii in dados_brutos:
        codigo_mercadoria = ii['commodityCode']
        pais = ii['countryCode']
        ano_mercado = ii['marketYear']
        ano_calendario = ii['calendarYear']
        mes = ii['month']
        id_atributo = ii['attributeId']
        id_unidade = ii['unitId']
        valor = ii['value']
        
        dict_valor = {
            'codigo_mercadoria': codigo_mercadoria,
            'pais': pais,
            'ano_mercado': ano_mercado,
            'ano_calendario': ano_calendario,
            'mes': mes,
            'id_atributo': id_atributo,
            'id_unidade' : id_unidade,
            'valor': valor
        }
        dados_limpos.append(dict_valor)
    return dados_limpos
    
def gerando_dataframe(dados_limpos, df, ajust):
    if df is None:
        df = pd.DataFrame(dados_limpos)
        if df.empty:
            return df
        if ajust == 1:
            df = df[df['ano_mercado'].isin(lista_periodo)]
        df['data'] = pd.to_datetime(df['ano_mercado'].astype(str) + df['mes'].astype(str).str.zfill(2) + '01', format='%Y%m%d')
        df['data'] = df['data'].dt.strftime('%d/%m/%Y')
        df['pais'] = df['pais'].map(country_dict)
        if ajust == 0:
            df['mercadoria'] = df['codigo_mercadoria'].astype(str).map(commodity_dict)
            df['nome_atributo'] = df['id_atributo'].map(name_attribute)
            df['unidade'] = df['id_unidade'].map(unidades_descricoes)
            df.drop(columns=['id_atributo', 'id_unidade'], inplace=True)
    else:
        df_novos = pd.DataFrame(dados_limpos)
        if ajust == 1:
            df_novos = df_novos[df_novos['ano_mercado'].isin(lista_periodo)]
        if df_novos.empty:
            return df
        df_novos['data'] = pd.to_datetime(df_novos['ano_mercado'].astype(str) + df_novos['mes'].astype(str).str.zfill(2) + '01', format='%Y%m%d')
        df_novos['data'] = df_novos['data'].dt.strftime('%d/%m/%Y')
        
        df_novos['mercadoria'] = df_novos['codigo_mercadoria'].astype(str).map(commodity_dict)
        df_novos['pais'] = df_novos['pais'].map(country_dict)
        if ajust == 0:
            df_novos['mercadoria'] = df_novos['codigo_mercadoria'].astype(str).map(commodity_dict)
            df_novos['nome_atributo'] = df_novos['id_atributo'].map(name_attribute)
            df_novos['unidade'] = df_novos['id_unidade'].map(unidades_descricoes)
            df_novos.drop(columns=['id_atributo', 'id_unidade'], inplace=True)
        df = pd.concat([df, df_novos], ignore_index=True)
    
    return df    
df_prod = df_forn = df_distr = None
#PARTE DA PRODUÇÃO
'''
for codigo in lista_mercadoria:
    url_prod = f'https://api.fas.usda.gov/api/psd/commodity/{codigo}/dataReleaseDates?limit=1&api_key=Q21KkotejbFgwFF8IRVPEpF4kgmD0lw4WzJXPfc5'
    response = rq.get(url_prod)

    if response.status_code == 200:
        data = response.json()
    elif response.status_code == 429:
        print("API atingiu limite de requisições, tente novamente mais tarde:", response.status_code)
    else:
        print("Falha na requisição:", response.status_code)

    dados_limpos = tratando_dados_producao(data)
    df_prod = gerando_dataframe(dados_limpos, df_prod, 1)
    
print(df_prod)
df_prod.drop(columns=['mes', 'ano_mercado'], inplace=True)
df_prod.to_excel( ROOT_PATH / 'Prodution.xlsx', index=False)
wb_prod = openpyxl.load_workbook(ROOT_PATH / 'Prodution.xlsx')  
ws_prod = wb_prod.active

ajustar_colunas(ws_prod)
ajustar_bordas(wb_prod)
wb_prod.save(ROOT_PATH / 'Prodution.xlsx')
'''
#PARTE DO FORNECIMENTO
for codigo in lista_mercadoria:
    for ano in lista_periodo:
        url_sump = f'https://api.fas.usda.gov/api/psd/commodity/{codigo}/country/all/year/{ano}?limit=1&api_key=Q21KkotejbFgwFF8IRVPEpF4kgmD0lw4WzJXPfc5'        
        response = rq.get(url_sump)
        
        if response.status_code == 200:
            data_forn = response.json()
        elif response.status_code == 429:
            print("API atingiu limite de requisições, tente novamente mais tarde:", response.status_code)
            break
        else:
            print("Falha na requisição:", response.status_code)

        dados_limpos = tratando_dados_forn(data_forn)
        df_forn = gerando_dataframe(dados_limpos, df_forn, 0)

df_forn.drop(columns=['mes', 'ano_mercado'], inplace=True)


#PARTE DISTRIBUIÇÃO
'''
for codigo in lista_mercadoria:
    for ano in lista_periodo:
        for pais in lista_paises:
            url_sump = f'https://api.fas.usda.gov/api/psd/commodity/{codigo}/country/{pais}/year/{ano}?limit=1&api_key=Q21KkotejbFgwFF8IRVPEpF4kgmD0lw4WzJXPfc5'
            response = rq.get(url_sump)

            if response.status_code == 200:
                data_distr = response.json()
                # print(data_distr)
            elif response.status_code == 429:
                print("API atingiu limite de requisições, tente novamente mais tarde:", response.status_code)
            else:
                print("Falha na requisição:", response.status_code)

            dados_limpos = tratando_dados_forn(data_distr)
            df_distr = gerando_dataframe(dados_limpos, df_distr, 0)
df_distr.to_excel(ROOT_PATH / 'Distribuicao.xlsx', index=False)
wb_distr = openpyxl.load_workbook(ROOT_PATH / 'Distribuicao.xlsx')  
ws_distr = wb_distr.active

ajustar_colunas(ws_distr)
ajustar_bordas(wb_distr)
wb_distr.save(ROOT_PATH / 'Distribuicao.xlsx')
planilha_principal = openpyxl.Workbook()
arquivos = ["Prodution.xlsx", "Fornecimento.xlsx"]
nomes_abas = ["Prodution.xlsx", "Fornecimento.xlsx"]

for i, arquivo in enumerate(arquivos):
    wb = openpyxl.load_workbook(ROOT_PATH / arquivo)
    aba = planilha_principal.create_sheet(nomes_abas[i])
    for linha in wb.active.iter_rows(values_only=True):
        aba.append(linha)
    ajustar_colunas(aba)

for aba in planilha_principal.sheetnames:
    if aba not in nomes_abas:
        del planilha_principal[aba]

ajustar_bordas(planilha_principal)
planilha_principal.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\USDA.xlsx")

'''
#Faz autenticação do google drive para jogar os arquivos gerados
# CLIENT_SECRET_FILE = 'credencials.json'
# API_NAME = 'drive'
# API_VERSION = 'v3'
# SCOPES = ["https://www.googleapis.com/auth/drive"]

# service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

# #PASSA O ARQUIVO PARA O DRIVE
# file_id = "1e2yihre6trC07ai7IayhrvLrFP4c57za"
# FILE_NAMES = ["USDA.xlsx"]
# MIME_TYPES = ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]

# add_arquivos_a_pasta(FILE_NAMES, MIME_TYPES, service, file_id)

if __name__ == '__main__':
    from sql import executar_sql 
    executar_sql()