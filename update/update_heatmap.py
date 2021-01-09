#!/usr/bin/env python
# coding: utf-8


import os
import glob
import itertools

import pandas as pd
import seaborn as sns
import numpy as np

from matplotlib import pyplot
from matplotlib.colors import LogNorm, PowerNorm

np.warnings.filterwarnings('ignore')


def draw(X, Xf, title=None, title_secondary=None, norm=None, figsize=None, **kwargs):
    if norm is None:
        norm = PowerNorm(gamma=.2)

    if figsize is None:
        figsize=(30 / 2.54, 30 / 2.54)

    fig = pyplot.figure(figsize=figsize, facecolor='white')
    gs = fig.add_gridspec(len(X), 10)

    Xs = (X.T / X.max(axis=1)).T.copy()

    main_ax = fig.add_subplot(gs[0:, :-1])
    im = sns.heatmap(
        Xs,
        cmap='Spectral_r', cbar=False, alpha=.85,
        norm=norm, vmax=Xs.max().max(), ax=main_ax
    )
    main_ax.set_yticks(np.arange(Xs.shape[0]) + .5,)
    main_ax.set_yticklabels(Xs.index, va='center', fontsize=12)

    xticks = main_ax.get_xticks()
    xticks = np.arange(0.5, xticks[-1], 30)
    main_ax.set_xticks(xticks)

    main_ax.set_xticklabels([
        column.date() for column in Xs.iloc[:, xticks].columns
    ], rotation=0, fontsize=14)

    main_ax.set_title(title, fontsize=32)
    Xf = Xf.iloc[::-1]

    for idx in range(len(X)):
        row = Xf.iloc[idx:idx + 1]

        ax = fig.add_subplot(gs[idx, -1])
        ax = row.plot.barh(ax=ax)

        ax.set_xlim((0, Xf.max()))
        ax.axis('off')

        if idx == 0 and title_secondary is not None:
            ax.set_title(title_secondary)

    ax.axis('on')
    ax.tick_params(
        axis='y',
        labelleft=False,
        left=False
    )
    ax.set_ylabel('')

    for frame_border in ['top', 'right', 'left']:
        ax.spines[frame_border].set_visible(False)

    return main_ax


def load_data():
    df = pd.DataFrame([])
    tdf = pd.DataFrame([])

    for file_name in glob.glob('./*.csv'):
        dept_data = pd.read_csv(file_name)
        dept_data['fecha'] = pd.to_datetime(dept_data['fecha'])

        dept_data = dept_data.set_index(['cod_ine', 'fecha'])['confirmados'].unstack()
        tdf = pd.concat([tdf, dept_data])

        dept_data = dept_data.fillna(0).T
        dept_data = dept_data.rolling(window=14).sum().dropna(how='all').T.round()
        df = pd.concat([df, dept_data])

    return df, tdf


if __name__ == '__main__':
    muni = pd.read_csv('./update/sdsn.gen.csv')
    muni = muni.set_index('cod_ine')

    cmun = muni[['municipio']]
    cmun = cmun[~cmun.index.duplicated(keep='first')]

    df, tdf = load_data()

    fdata = df[df.T.max() > 15]
    fdata = fdata.T.interpolate(method='quadratic').T
    fdata = fdata.fillna(0).round()
    fdata[fdata < 1] = 1e-3

    fdata = fdata.loc[fdata.idxmax(axis=1).sort_values(ascending=False).index]
    pdata = (1e5 * tdf.loc[fdata.index].sum(axis=1).div(muni['poblacion']))
    pdata = pdata[~pdata.isnull()]
    pdata = pdata[fdata.index]

    fdata.index = [_ for _ in itertools.chain(*cmun.loc[fdata.index].to_numpy())]

    ax = draw(
        fdata,
        pdata[::-1],
        title='Casos activos por municipio ({})'.format(str(fdata.columns[-1])[:10]),
        title_secondary='Casos / 100k hab.',
        norm=PowerNorm(.66),
        figsize=(120 / 2.54, 120 / 2.54)
    )

    ax.get_figure().savefig('./plots/peaks.jpg', bbox_inches='tight')
