#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import glob

import unidecode
import numpy as np
import pandas as pd

from src import config
from src.data import cnbv_inclusion_financiera

DATA_FOLDER = os.path.join(
    config.PRJ_DIR,
    'data/interim/cnbv/inclusion_financiera/')

os.makedirs(name=DATA_FOLDER, exist_ok=True)


def get_csvs():
    """
    """

    sheet_names = glob.glob(os.path.join(
        cnbv_inclusion_financiera.DATA_FOLDER, '*'))
    csv_files = []
    for sheet in sheet_names:
        sheet_path = os.path.join(
            cnbv_inclusion_financiera.DATA_FOLDER,
            sheet,
            '*')

        csv_files += glob.glob(sheet_path)

    return csv_files


def get_valid_columns(df, min_null=0.9):
    """
    """
    # Drop columns that are empty
    null_count_per_colu = df.replace(
        '', np.nan
    ).isnull().sum(axis=0)

    null_percent_per_col = null_count_per_colu / df.shape[0]

    is_valid_columns = null_percent_per_col < min_null
    valid_columns = is_valid_columns[is_valid_columns].index
    not_valid_columns = is_valid_columns[~is_valid_columns].index

    df_valid_cols = df[valid_columns].copy()
    df_not_valid_cols = df[not_valid_columns].copy()

    return df_valid_cols, df_not_valid_cols


def get_valid_rows(df, min_null=.5):
    """
    """

    null_count_per_row = df.replace(
        '', np.nan
    ).isnull().sum(axis=1)

    null_percent_per_row = null_count_per_row / df.shape[1]

    is_valid_row = null_percent_per_row < min_null
    valid_rows = is_valid_row[is_valid_row].index
    not_valid_rows = is_valid_row[~is_valid_row].index

    df_valid_cols = df.loc[valid_rows]
    df_not_valid_cols = df.loc[not_valid_rows]

    return df_valid_cols, df_not_valid_cols


def clean_col_name(txt):
    """
    """
    txt = txt.lower()
    txt = unidecode.unidecode(txt)
    txt = txt.replace('\n', ' ')
    txt = txt.replace('  ', ' ')
    txt = txt.strip(' ')
    txt = txt.replace(' ', '_')

    return txt


def rename_columns(df, df_rows_dropped):
    """
    """
    macro_name = df_rows_dropped.dropna(
        how='all'
    ).iloc[-1].fillna(method='ffill').fillna('')

    columns = df.iloc[0]

    new_col_name = macro_name + '__' + columns
    new_col_name = [
        x[2:] if '__' == x[:2] else x
        for x in new_col_name]

    # Append number if columns are duplicated
    count_columns = {}
    columns_unique = []
    for item in new_col_name:
        item = clean_col_name(item)
        if item not in count_columns:
            count_columns[item] = 0

        count_columns[item] += 1

        if count_columns[item] > 1:
            item += ' ({0})'.format(count_columns[item])
        columns_unique.append(item)

    df.columns = columns_unique
    df = df.iloc[1:].copy()

    return df


def process(df):
    """
    """
    csv_files = get_csvs()

    sheet_df = {}
    for path in csv_files:
        print(path)

        df_raw = pd.read_csv(path)

        df, _ = get_valid_columns(df_raw)
        df, df_rows_dropped = get_valid_rows(df)
        df = rename_columns(
            df,
            df_rows_dropped)

        name = path.split('/')[-1]
        sheet_name = path.split('/')[-2]
        date = name.split('_')[-1].replace('.csv', '')

        df['name'] = name
        df['sheet_name'] = sheet_name
        df['date'] = date

        sheet_folder = os.path.join(
            DATA_FOLDER,
            sheet_name)
        os.makedirs(name=sheet_folder, exist_ok=True)

        df.to_csv(os.path.join(sheet_folder, name), index=False)

        if sheet_name not in sheet_df:
            sheet_df[sheet_name] = []

        sheet_df[sheet_name].append(df)

    for sheet_name, sheet_items in sheet_df.items():
        all_sheets_columns = pd.Series([
            col_name
            for x in sheet_items
            for col_name in x.columns
        ])

        all_sheets_columns_cnt = all_sheets_columns.value_counts()
        print(all_sheets_columns_cnt)

        sheet_dataset_path = os.path.join(
            DATA_FOLDER,
            sheet_name + '.csv')

        sheet_dataset = pd.concat(sheet_items, axis=0)

        sheet_dataset.to_csv(sheet_dataset_path, index=False)
        print(sheet_dataset_path)

    return df


if __name__ == '__main__':
    """
    """
    process()
