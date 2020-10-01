
# -*- coding: utf-8 -*-
import csv
import os
import time
import datetime
import utils
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki
import pandas as pd


def main():
    # morph.io requires this db filename, but scraperwiki doesn't nicely
    # expose a way to alter this. So we'll fiddle our environment ourselves
    # before our pipeline modules load.
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

    # a data de referência será sempre d-2
    data_referencia = datetime.date.today() - datetime.timedelta(days=2)

    # se não é dia útil
    if not utils.isbizday(data_referencia):
        print('Nenhum dado capturado, pois a data não é dia útil', data_referencia)
        return False

    
    url_base = 'http://dados.cvm.gov.br/dados/FI/CAD/DADOS/inf_cadastral_fi_{}.csv'
    url = url_base.format(data_referencia.strftime('%Y%m%d'))
    #print(url)

    df = pd.read_csv(
        url,
        sep=';',
        encoding='latin1'
    )

    # transforma o campo CO_PRD
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace('.','')
    df['CO_PRD'] = df['CO_PRD'].str.replace('/','')
    df['CO_PRD'] = df['CO_PRD'].str.replace('-','')
    df['CO_PRD'] = df['CO_PRD'].str.zfill(14)

    df['DT_REG'] = pd.to_datetime(df['DT_REG'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_CONST'] = pd.to_datetime(df['DT_CONST'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_CANCEL'] = pd.to_datetime(df['DT_CANCEL'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_INI_SIT'] = pd.to_datetime(df['DT_INI_SIT'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_INI_ATIV'] = pd.to_datetime(df['DT_INI_ATIV'], errors='coerce').dt.strftime('%Y-%m-%d')

    #print(df.columns.tolist())
    df = df.astype(str)
    
    for row in df.to_dict('records'):
        try:
            scraperwiki.sqlite.save(unique_keys=df.columns.values.tolist(), data=row)
        except Exception as e:
            print('Erro', e)
            continue

    # rename file
    time.sleep(60)
    print('Renomeando arquivo sqlite')
    os.rename('scraperwiki.sqlite', 'data.sqlite')

    #print('Registros importados com sucesso')
    return True


if __name__ == '__main__':
    main()

