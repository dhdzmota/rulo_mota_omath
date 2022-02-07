#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import numpy as np
import os

from src import config
from src.data.utils import parallel
from src.features.utils import get_config


DATA_GROUPER = None
FEATURE_LIST = None


DATA_PATH= os.path.join(
    config.PRJ_DIR,
    'data/processed/X.csv')

def convert_to_date(date):
    """
    """
    date = str(date)
    date = datetime.datetime(
        int(date[0:4]),
        int(date[4:]), 1)

    return date


def get_mun_features(mun_id):
    """
    """
    global DATA_GROUPER, FEATURE_LIST

    mun_data = DATA_GROUPER.get_group(mun_id)

    X = []
    for year, year_data in mun_data.groupby('year'):
        if year == 2021:
            continue

        Q1 = year_data[year_data['month'] == 3]
        Q2 = year_data[year_data['month'] == 6]
        Q3 = year_data[year_data['month'] == 9]
        Q4 = year_data[year_data['month'] == 12]

        x = {
            'year': year,
            'mun': mun_data['informacion_general__clave_municipio'].iloc[0]}

        for feature in FEATURE_LIST:
            q1 = Q1[feature].iloc[0]
            q2 = Q2[feature].iloc[0]
            q3 = Q3[feature].iloc[0]
            q4 = Q4[feature].iloc[0]

            qs = np.array([q1, q2, q3, q4])

#            qs = np.array([1, 1, 1, 1])
#            if np.abs(np.diff(qs)).sum() > 0.0001:
            x['%s_delta' % feature] = q4 - q1
            x['%s_gradient' % feature] = np.gradient(qs).mean()
            x[feature] = q1

#            x['%s_mean_prop' % feature] = ((
#                q2 / (q1 + 1e-6)
#            ) + (
#                q3 / (q2 + 1e-6)
#            ) + (
#                q4 / (q3 + 1e-6)
#            )) / 3
#            x['%s_prop' % feature] = q4 / (q1 + 1e-6)
        X.append(x)

    X = pd.DataFrame(X)

    return X


def get(data, feature_list):
    """
    """
    global DATA_GROUPER, FEATURE_LIST

    data['date'] = data['date'].apply(convert_to_date)

    data['month'] = data['date'].dt.month
    data['year'] = data['date'].dt.year
    data['informacion_general__clave_municipio'] = data[
        'informacion_general__clave_municipio'].astype(int).astype(str)

    data = data[
        data['informacion_general__municipio'] !=  'Sin identificar']

    data.fillna(0, inplace=True)

    DATA_GROUPER = data.groupby(
        'informacion_general__clave_municipio')

    FEATURE_LIST = feature_list
    clave_municipio = data['informacion_general__clave_municipio'].unique()

    mun_features = parallel.apply(
        get_mun_features,
        clave_municipio)

    mun_features = pd.concat(mun_features)

    mun_features['id'] = (
        mun_features['mun'].astype('str')
    ) + '-' + (
        mun_features['year'].astype('str'))

    mun_features.set_index('id', inplace=True)

    return mun_features


if __name__ == '__main__':
    """
    """

    path = os.path.join(
        config.PRJ_DIR,
        'data/interim/cnbv/inclusion_financiera/')

    config_features = get_config()

    data_features = []
    for key in config_features.keys():
        path_dataset = path+key
        feature_list = config_features[key]

        data = pd.read_csv(path_dataset)

        data_features.append(get(data, feature_list))

    X = pd.concat(data_features, axis=1)

    X.to_csv(DATA_PATH,
             encoding='utf-8',
             mode='w')


