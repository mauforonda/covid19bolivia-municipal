#!/usr/bin/env python3

import time
import requests
import pandas as pd
import datetime as dt
from unicodedata import normalize
import json


URL = 'https://siip.produccion.gob.bo/repSIIP2/JsonAjaxCovid.php?flag=contagiados&num_dias={}'
DAYS = 14
SLEEP_T = 10
TIMEOUT = 10
MAX_TRY = 5


def sanitize_response(response):
    return json.loads(response.text
                      .replace(',"coordinates":}', '}')
                      .replace('"pob2020": ,', '')
                      .replace('"_pib2006": ,', ''))


def do_download(url, _try=0):
    try:
        return requests.get(url, timeout=TIMEOUT)
    except:
        if _try > MAX_TRY:
            return
        return do_download(url, _try + 1)


def download():
    json_keys = ['cod_mun', '_fecha_ultimo', '_f_0709202']
    df = pd.DataFrame([])

    for num_dias in range(1, DAYS + 1):
        response = do_download(URL.format(num_dias))

        if response is None:
            continue

        ddf = pd.DataFrame([
            {
                key: feature['properties'][key] for key in json_keys
            } for feature in sanitize_response(response)['data_mapa']['features']
        ])
        ddf['num_dias'] = num_dias - 1

        df = pd.concat([df, ddf])
        time.sleep(SLEEP_T)

    return df.reset_index(drop=True)


MES = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic']
MES = {mes:(idx + 1) for idx, mes in enumerate(MES)}

def format(df):
    df.columns = ['cod_ine', 'fecha', 'confirmados', 'num_dias']

    df.cod_ine = df.cod_ine.astype(int)
    df.confirmados = df.confirmados.astype(int)
    df.fecha = df.fecha.apply(
        lambda _: dt.datetime(
            2021, MES[_.split(' ')[-1].lower()[:3]], int(_.split(' ')[0])
        )
    )

    # elimina dias sin actualizacion
    df = df.sort_values(['cod_ine', 'fecha', 'num_dias'])
    df = df[~df.iloc[:, :3].duplicated(keep='first')]

    # ajusta la fecha
    df['fecha_temporal'] = df['fecha'].max() - df['num_dias'].apply(
        lambda _: pd.Timedelta(days=_)
    )
    df['fecha'] = df['fecha'].where(df['fecha'] < df['fecha_temporal'], df['fecha_temporal'])
    df = df.drop(['num_dias', 'fecha_temporal'], axis=1)

    # saca la diferencia entre casos acumulados <-
    df = df.set_index('cod_ine')
    final_df = pd.DataFrame([])

    for muni_key, muni_df in df.groupby(level=0):
        muni_df = muni_df.set_index('fecha', append=True)
        muni_df = muni_df.sort_values('confirmados').diff().fillna(muni_df)

        final_df = pd.concat([final_df, muni_df])

    final_df = final_df.reset_index()
    final_df.index = final_df['cod_ine']

    return final_df


def get_data():
    return format(download())


def rehydrate(df):
    extra = 'update/mun_ine.csv'

    df = df.join(pd.read_csv(extra, index_col='cod_ine'))
    df[['cod_ine', 'confirmados']] = df[['cod_ine', 'confirmados']].astype(int)

    return df


def save(df):
    column_order = ['fecha', 'cod_ine', 'municipio', 'confirmados']

    for departamento in df.departamento.unique():
        fn = normalize(u'NFKD', '{}.csv'.format(
            departamento.lower().replace(' ','_')
        )).encode('ascii', 'ignore').decode('utf8')

        pd.concat([
            pd.read_csv(fn, parse_dates=['fecha']),
            df[df.departamento == departamento][column_order]
        ], axis=0).sort_values(['fecha', 'cod_ine']).drop_duplicates(
            subset=['cod_ine','fecha']
        ).to_csv(fn, index=False, date_format='%Y-%m-%d')


if __name__ == '__main__':
    df = get_data()
    save(rehydrate(df))
