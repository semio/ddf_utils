# -*- coding: utf-8 -*-

"""functions for common tasks on ddf datasets"""

import pandas as pd


def _translate_column_inline(df, column, target_column, dictionary, not_found):

    df_new = df.copy()

    if not_found == 'drop':
        df_new[target_column] = df_new[column].map(
            lambda x: dictionary[x] if x in dictionary.keys() else None)
    if not_found == 'error':
        df_new[target_column] = df_new[column].map(
            lambda x: dictionary[x])
    if not_found == 'include':
        df_new[target_column] = df_new[column].map(
            lambda x: dictionary[x] if x in dictionary.keys() else x)

    return df_new


def _translate_column_df(df, column, target_column, dictionary, base_df, not_found):

    mapping = {}
    no_match = []
    bools = []

    search_cols = dictionary['key']
    base_df = base_df.set_index(dictionary['value'])

    if isinstance(search_cols, str):
        search_cols = [search_cols]

    for sc in search_cols:
        bools.append(base_df[sc] == f)

    mask = bools[0]
    for m in bools[1:]:
        mask = mask | m
    filtered = base_df[mask]

    if len(filtered) == 1:
        mapping[f] = filtered.index[0]
    elif len(filtered) > 1:
        logging.warning("multiple match found: "+f)
        mapping[f] = filtered.index[0]
    else:
        no_match.append(f)

    base_df = base_df.reset_index()

    if not_found == 'error' and len(no_match) > 0:
        raise ValueError('missing keys in dictionary. please check your input.')
    else:
        return _translate_column_inline(df, column, target_column, dictionary, not_found)


def translate_column(df, column, dictionary_type, dictionary,
                     target_column=None, base_df=None, not_found='drop'):
    """doc here"""

    if dictionary_type not in ['inline', 'file', 'dataframe']:
        raise ValueError("dictionary_type should be one of 'inline', 'file', 'dataframe'")

    if not_found not in ['drop', 'error', 'include']:
        raise ValueError("not_found should be one of 'drop', 'error', 'include'")

    if dictionary_type == 'dataframe' and base_df is None:
        raise ValueError("please specify base_df when dictionary type is 'dataframe'")

    if not target_column:
        target_column = column

    if dictionary_type == 'inline':
        df_new = _translate_column_inline(df, column, target_column, dictionary, not_found)
    if dictionary_type == 'file':
        import json
        with open(dictionary) as f:
            d = json.load(f)
        df_new = _translate_column_inline(df, column, target_column, d, not_found)
    if dictionary_type == 'dataframe':
        assert 'key' in dictionary.keys() and 'value' in dictionary.keys()
        df_new = _translate_column_df(df, column, target_column, dictionary, not_found)

    return df_new
