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
    'https://api.datamexico.org/tesseract/cubes/'
    'banxico_mun_income_remittances/aggregate.jsonrecords?'
    'drilldowns%5B%5D=Geography+Municipality.Geography.Municipality&'
    'drilldowns%5B%5D=Time.Date.Year&measures%5B%5D=Remittance+Amount&'
    'parents=false&sparse=false')


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

