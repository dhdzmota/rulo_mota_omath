#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 27 11:57:32 2022
"""
import os
import json
import requests
import pandas as pd

from src import config


DATA_PATH = os.path.join(
    config.PRJ_DIR,
    'data/raw/imss_credits.csv')


api_url = (
   'https://api.datamexico.org/tesseract/cubes/'
   'imss_credits/aggregate.jsonrecords?'
   'drilldowns%5B%5D=Sex.Sex.Sex&'
   'drilldowns%5B%5D=Geography+Municipality.Geography.Municipality&'
   'measures%5B%5D=Credits&'
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


if __name__ == '__main__':
    download()
