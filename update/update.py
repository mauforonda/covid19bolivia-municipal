#!/usr/bin/env python3

import requests
import pandas as pd
import datetime as dt

def download():
    url = 'https://siip.produccion.gob.bo/repSIIP2/JsonAjaxCovid.php?flag=contagiados&num_dias=1'
    json_keys = ['cod_mun', '_fecha_ultimo', '_f_0709202']

    return pd.DataFrame([{key: feature['properties'][key] for key in json_keys} for feature in requests.get(url).json()['data_mapa']['features']])
    
def format(df):
    mes = {'ago':8, 'oct':10, 'may':5, 'jul':7, 'sep':9, 'jun':6, 'nov':11, 'abr':4, 'dic':12, 'mar':3, 'ene':1, 'feb':2}
    column_names = ['cod_ine', 'fecha', 'confirmados']
    
    df.columns = column_names
    df.cod_ine = df.cod_ine.astype(int)
    df.index = df.cod_ine
    df.fecha = df.fecha.apply(lambda _: dt.datetime(2021, mes[_.split(' ')[-1].lower()[:3]], int(_.split(' ')[0])))
    return df

def get_data():
    return format(download())

def rehydrate(df):
    extra = 'update/mun_ine.csv'
    
    df = pd.concat([pd.read_csv(extra, index_col='cod_ine'), df], axis=1).dropna()
    df[['cod_ine', 'confirmados']] = df[['cod_ine', 'confirmados']].astype(int)
    return df

def save(df):
    column_order = ['fecha', 'cod_ine', 'municipio', 'confirmados']
    
    for departamento in df.departamento.unique():
        fn = '{}.csv'.format(departamento.lower().replace(' ','_'))
        pd.concat([pd.read_csv(fn, parse_dates=['fecha']), df[df.departamento == departamento][column_order]], axis=0).drop_duplicates(subset=['cod_ine','fecha']).to_csv(fn, index=False, date_format='%Y-%m-%d')

df = get_data()
save(rehydrate(df))
