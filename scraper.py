
# -*- coding: utf-8 -*-
import csv
import datetime
import os
import time

import pandas as pd
import scraperwiki
from sqlalchemy import create_engine

import utils

os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'


def main():
    # a data de referência será sempre d-2
    data_referencia = datetime.date.today() - datetime.timedelta(days=2)

    # se não é dia útil
    if not utils.isbizday(data_referencia):
        print('Nenhum dado capturado, pois a data não é dia útil', data_referencia)
        return False

    url_base = 'http://dados.cvm.gov.br/dados/FI/CAD/DADOS/inf_cadastral_fi_{}.csv'
    url = url_base.format(data_referencia.strftime('%Y%m%d'))

    df = pd.read_csv(
        url,
        sep=';',
        encoding='latin1'
    )

    # transforma o campo CO_PRD
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace('.', '')
    df['CO_PRD'] = df['CO_PRD'].str.replace('/', '')
    df['CO_PRD'] = df['CO_PRD'].str.replace('-', '')
    df['CO_PRD'] = df['CO_PRD'].str.zfill(14)

    df['DT_REG'] = pd.to_datetime(
        df['DT_REG'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_CONST'] = pd.to_datetime(
        df['DT_CONST'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_CANCEL'] = pd.to_datetime(
        df['DT_CANCEL'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_INI_SIT'] = pd.to_datetime(
        df['DT_INI_SIT'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_INI_ATIV'] = pd.to_datetime(
        df['DT_INI_ATIV'], errors='coerce').dt.strftime('%Y-%m-%d')

    # print(df.columns.tolist())
    df = df.astype(str)

    engine = create_engine('sqlite:///data.sqlite', echo=True)
    sqlite_connection = engine.connect()
    print('Importando usando pandas to_sql')
    df.to_sql(
        'swdata',
        sqlite_connection,
        if_exists='replace',
        index=False
    )


if __name__ == '__main__':
    main()
