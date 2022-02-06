# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from src.data import banxico_mun_income_remittances


def get():
    """
    """
    data = banxico_mun_income_remittances.get()

    # Parse data types
    data['Municipality ID'] = data['Municipality ID'].astype(int).astype(str)
    data['Year'] = data['Year'].astype(int)

    # Add row id
    data['id'] = data['Municipality ID'] + '-' + data['Year'].astype(str)
    data.drop_duplicates('id', keep='first', inplace=True)

    new_data = []
    for mun_id, data_mun in data.groupby('Municipality ID'):
        data_mun.sort_values('Year', inplace=True)

        # Remittance Amount Percent Change
        data_mun['remittance_percent_change'] = data_mun[
            'Remittance Amount'].pct_change().values

        # is change percent negative
        is_change_neg = data_mun['remittance_percent_change'] < 0

        # is change percent negative at least 5 percent
        data_mun['is_change_neg_at_5'] = (
            is_change_neg
        ) & (
            data_mun['remittance_percent_change'].abs() >= .05
        )

        # is change percent negative at least 10 percent
        data_mun['is_change_neg_at_10'] = (
            is_change_neg
        ) & (
            data_mun['remittance_percent_change'].abs() >= .1
        )

        # is change percent negative at least 25 percent
        data_mun['is_change_neg_at_25'] = (
            is_change_neg
        ) & (
            data_mun['remittance_percent_change'].abs() >= .25
        )

        new_data.append(
            data_mun.iloc[1:])

    new_data = pd.concat(new_data)
    new_data.set_index('id', inplace=True)

    # Is remittance_percent_change a valid value
    is_invalid_municipality = (
        new_data['remittance_percent_change'].isnull()
    ) | (
        new_data['remittance_percent_change'].abs().apply(np.isinf)
    )
    # Municipalities were remittance_percent_change is a valid value
    invalid_municipality = new_data[
        is_invalid_municipality
    ]['Municipality ID'].unique()

    # Filter municipalities were remittance_percent_change is a valid value
    new_data[
        ~new_data['Municipality ID'].isin(invalid_municipality)
    ]['Municipality ID'].nunique()

    new_data = new_data[
        ~new_data['Municipality ID'].isin(invalid_municipality)]

    new_data['Remittance Amount Million USD'] = new_data[
        'Remittance Amount'] / 1e+6

    return new_data
