
# -*- coding: utf-8 -*-
import csv
import os
import datetime
import utils
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
    print(url)

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

    print(df.columns.tolist())

    for row in df.to_dict('records'):
        scraperwiki.sqlite.save(unique_keys=['CO_PRD'], data=row)

    # rename file
    os.rename('scraperwiki.sqlite', 'data.sqlite')

    print('Registros importados com sucesso')
    return True


if __name__ == '__main__':
    main()
