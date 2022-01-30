#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 27 11:57:32 2022
"""
import os
import json
import requests
import pandas as pd


DATA_PATH =

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
    req = requests.get(api)
    json_data = json.loads(req.text)

    data = pd.json_normalize(json_data['data'])

    data.to_csv()

    data = data[data['Sex'] != 'Not Specified']

    data['Municipality ID'].value_counts()

    data['Sex'].value_counts()



