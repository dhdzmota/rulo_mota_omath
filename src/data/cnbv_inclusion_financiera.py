# -*- coding: utf-8 -*-
import os
import glob

import requests
from bs4 import BeautifulSoup
import xlrd
import pandas as pd

from src import config

# Path where to store datasets
DATA_FOLDER = os.path.join(
    config.PRJ_DIR,
    'data/raw/cnbv/inclusion_financiera/')
os.makedirs(name=DATA_FOLDER, exist_ok=True)

# The web-site where to find the href links to the datasets.
URL = (
   'https://www.gob.mx/cnbv/'
   'acciones-y-programas/'
   'bases-de-datos-de-inclusion-financiera')


def get_hyperlinks(html):
    """Get hyperlinks from html.

    Returns
    --------
    hyperlinks : list
        List of hyperlinks.
    """
    # Parse html
    soup = BeautifulSoup(html)

    # Get hyperlinks
    hyperlinks = []
    for a_element in soup.find_all('a'):
        dir(a_element)

        href = a_element.get('href', None)
        if href is not None:
            hyperlinks.append(href)

    return hyperlinks


def download():
    """Download inclusion financiera csv files.
    """

    # Get html
    req = requests.get(URL)
    html = req.content

    # Get hyperlinks
    hyperlinks = get_hyperlinks(html)

    # Get hyperlinks of excel files
    excel_links = [
        href
        for href in hyperlinks
        if 'xls' in href]

    for excel_path in excel_links:
        print(excel_path)
        # Download excel
        req_excel_file_content = requests.get(excel_path)
        excel = xlrd.open_workbook(
            file_contents=req_excel_file_content.content)

        # Get excel filename
        excel_name = excel_path.split('/')[-1].replace('.xlsm', '')

        sheets = excel.sheet_names()
        sheets = [x for x in sheets if 'Mun' in x]

        # Get sheets
        for sheet in sheets:
            # sheet foler path
            sheet_folder_path = os.path.join(
                DATA_FOLDER, sheet)
            os.makedirs(sheet_folder_path, exist_ok=True)

            # sheet csv path
            data_path = os.path.join(
                sheet_folder_path, '{0}.csv'.format(excel_name))

            # get sheet as a pandas df
            sheet_df = pd.DataFrame(
                excel.sheet_by_name(
                    sheet
                )._cell_values)
            # save sheet as csv
            sheet_df.to_csv(data_path, index=False)


if __name__ == '__main__':
    """
    """
    download()
