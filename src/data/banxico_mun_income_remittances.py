#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import pandas as pd

from src import config


DATA_PATH = os.path.join(
    config.PRJ_DIR,
    'data/raw/banxico_mun_income_remittances.csv')


api_url = (
    'https://api.datamexico.org/tesseract/data.jsonrecords?'
    'cube=banxico_mun_income_remittances&'
    'drilldowns=Municipality%2CYear&'
    'measures=Remittance+Amount&'
    'parents=false&'
    'sparse=false')


def download():
    """
    """
    req = requests.get(api_url)

    json_data = json.loads(req.text)

    data = pd.json_normalize(json_data['data'])

    data.to_csv(DATA_PATH, index=False)


def get():
    """
    """
    data = pd.read_csv(DATA_PATH)

    return data


if __name__ == '__main__':
    download()
