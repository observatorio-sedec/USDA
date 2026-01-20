import psycopg2
from ETL import df_forn
from conexao import conexao

def executar_sql():
    cur = conexao.cursor()
    
    cur.execute('SET search_path TO usda, public')
    # Verifica a existência das tabelas e retorna 1

    verificando_existencia_usda_global= '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'usda' AND table_type='BASE TABLE' AND table_name='usda_global';
    '''
    
    usda_global = '''
    CREATE TABLE IF NOT EXISTS usda.USDA_GLOBAL(
        id_usda_global SERIAL PRIMARY KEY,
        codigo_mercadoria INTEGER NOT NULL,
        pais TEXT,
        ano_calendario INTEGER, 
        valor INTEGER,
        mercadoria TEXT,
        data DATE,
        nome_atributo TEXT,
        unidade TEXT);
    '''

    cur.execute(usda_global)

    cur.execute(verificando_existencia_usda_global)
    resultado_usda_global= cur.fetchone()

    if resultado_usda_global[0] == 1:
        dropando_tabela_usda_gobal = '''
        TRUNCATE TABLE usda.USDA_GLOBAL
        '''
        cur.execute(dropando_tabela_usda_gobal)
    else:
        pass

    #INSERINDO DADOS
    inserindo_usda_global = \
    '''
    INSERT INTO usda.USDA_GLOBAL(codigo_mercadoria, pais, ano_calendario, valor, data, mercadoria, nome_atributo, unidade)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df_forn.iterrows():
            dados = (
                i['codigo_mercadoria'], 
                i['pais'], 
                i['ano_calendario'], 
                i['valor'], 
                i['data'],
                i['mercadoria'],
                i['nome_atributo'],
                i['unidade']
            )
            cur.execute(inserindo_usda_global, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")

    conexao.commit()
    conexao.close()