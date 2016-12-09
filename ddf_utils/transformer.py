# -*- coding: utf-8 -*-

"""functions for common tasks on ddf datasets"""

import pandas as pd
import numpy as np
import json
import logging


def _translate_column_inline(df, column, target_column, dictionary, not_found):

    df_new = df.copy()

    if not_found == 'drop':
        df_new[target_column] = df_new[column].map(
            lambda x: dictionary[x] if x in dictionary.keys() else None)
        df_new = df_new.dropna(subset=[target_column])
    if not_found == 'error':
        df_new[target_column] = df_new[column].map(
            lambda x: dictionary[x])
    if not_found == 'include':
        df_new[target_column] = df_new[column].map(
            lambda x: dictionary[x] if x in dictionary.keys() else x)

    return df_new


def _translate_column_df(df, column, target_column, dictionary, base_df, not_found):

    mapping = dict()
    no_match = set()

    search_cols = dictionary['key']
    if isinstance(search_cols, str):
        search_cols = [search_cols]

    assert isinstance(dictionary['value'], str)
    base_df = base_df.set_index(dictionary['value'])

    for f in df[column].values:
        bools = []
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
            no_match.add(f)

    base_df = base_df.reset_index()

    if len(no_match) > 0:
        logging.warning('no match found: ' + str(no_match))

    if not_found == 'error' and len(no_match) > 0:
        raise ValueError('missing keys in dictionary. please check your input.')
    else:
        return _translate_column_inline(df, column, target_column, mapping, not_found)


def translate_column(df, column, dictionary_type, dictionary,
                     target_column=None, base_df=None, not_found='drop'):
    """change values in a column base on a mapping dictionary.

    The dictionary can be provided as a python dictionary, pandas dataframe or read from file.

    Parameters
    ----------
    df : `DataFrame`
        The dataframe to be translated
    column : `str`
        The column to be translated
    dictionary_type : `str`
        The type of dictionary, choose from `inline`, `file` and `dataframe`
    dictionary : `str` or `dict`
        The dictionary. Depanding on the `dictionary_type`, the value of this parameter should be:
        `inline`: `dict`
        `file`: the file path, `str`
        `dataframe`: `dict`, must have `key` and `value` keys. see examples in examples section.
    target_column : `str`, optional
        The column to store translated resluts. If this is None, then the one set with `column` will be replaced.
    `base_df` : `DataFrame`, optional
        When `dictionary_type` is `dataframe`, this option should be set
    `not_found`: `str`
        What to do if key in the dictionary is not found in the dataframe to be translated.
        avaliable options are `drop`, `error`, `include`

    Examples
    --------
    >>> df = pd.DataFrame([['geo', 'Geographical places'], ['time', 'Year']], columns=['concept', 'name'])
    >>> df
      concept                 name
    0     geo  Geographical places
    1    time                 Year
    >>> tf.translate_column(df, 'concept', 'inline', {'geo': 'country', 'time': 'year'})
       concept                 name
    0  country  Geographical places
    1     year                 Year

    >>> base_df = pd.DataFrame([['geo', 'country'], ['time', 'year']], columns=['concept', 'alternative_name'])
    >>> base_df
      concept alternative_name
    0     geo          country
    1    time             year
    >>> tf.translate_column(df, 'concept', 'dataframe',
    ...                     {'key': 'concept', 'value': 'alternative_name'},
    ...                     target_column='new_name', base_df=base_df)
      concept                 name new_name
    0     geo  Geographical places  country
    1    time                 Year     year
    """

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
        with open(dictionary) as f:
            d = json.load(f)
        df_new = _translate_column_inline(df, column, target_column, d, not_found)
    if dictionary_type == 'dataframe':
        assert 'key' in dictionary.keys() and 'value' in dictionary.keys()
        df_new = _translate_column_df(df, column, target_column, dictionary, base_df, not_found)

    return df_new


def translate_header(df, dictionary, dictionary_type='inline'):
    """change the headers of a dataframe base on a mapping dictionary.

    Parameters
    ----------
    df : `DataFrame`
        The dataframe to be translated
    dictionary_type : `str`, default to `inline`
        The type of dictionary, choose from `inline` or `file`
    dictionary : `dict` or `str`
        The mapping dictionary or path of mapping file
    """
    if dictionary_type == 'inline':
        return df.rename(columns=dictionary)
    elif dictionary_type == 'file':
        with open(dictionary, 'utf8') as f:
            dictionary = json.load(f)
        return df.rename(columns=dictionary)
    else:
        raise ValueError('dictionary not supported: '+dictionary_type)


def aagr(df: pd.DataFrame, window: int=10):  # TODO: create a op.py file for this kind of functions?
    """average annual growth rate

    Parameters
    ----------
    window : `int`
        the rolling window size

    Returns
    -------
    return : `DataFrame`
        The rolling apply result
    """
    pct = df.pct_change()
    return pct.rolling(window).apply(np.mean).dropna()


def trend_bridge(old_data, new_data, bridge_length):
    """smoothing data between series.

    To avoid getting artificial stairs in the data, we smooth between to
    series. Sometime one source is systematically higher than another source,
    and if we jump from one to another in a single year, this looks like an
    actual change in the data.

    Parameters
    ----------
    old_data : Series
    new_data : Series
    bridge_length : int
        the length of bridge

    Returns
    -------
    bridge_data : the bridged data
    """
    assert new_data.index[0] < old_data.index[-1]  # assume old data and new data have overlaps

    bridge_end = new_data.index[0]
    bridge_start = bridge_end - bridge_length

    assert bridge_start > old_data.index[0]

    bridge_height = new_data.ix[bridge_end] - old_data.ix[bridge_end]
    fraction = bridge_height / bridge_length

    bridge_data = old_data.copy()

    for i, row in bridge_data.ix[bridge_start:bridge_end].iteritems():
        if i == bridge_end:
            break
        bridge_data.ix[i:bridge_end] = bridge_data.ix[i:bridge_end] + fraction

    return bridge_data


def extract_concepts(dfs, base=None, join='full_outer'):
    if base is not None:
        concepts = base.set_index('concept')
    else:
        concepts = pd.DataFrame([], columns=['concept', 'concept_type']).set_index('concept')

    new_concepts = set()

    for df in dfs:
        for c in df.columns:
            new_concepts.add(c)
            if c in concepts.index:  # if the concept is in base, just use base data
                continue
            if np.issubdtype(df[c].dtype, np.number):
                concepts.ix[c, 'concept_type'] = 'measure'
            else:  # TODO: add logic for concepts/entities ingredients
                concepts.ix[c, 'concept_type'] = 'string'
    if join == 'ingredients_outer':
        # ingredients_outer join: only keep concepts appears in ingredients
        concepts = concepts.ix[new_concepts]
    return concepts.reset_index()
