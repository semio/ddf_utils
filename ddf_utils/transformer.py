# -*- coding: utf-8 -*-

"""functions for common tasks on ddf datasets"""

import pandas as pd
import numpy as np
import json
import logging

from ddf_utils.chef.helpers import prompt_select


def _translate_column_inline(df, column, target_column, dictionary,
                             not_found, ambiguity):
    df_new = df.copy()

    # check for ambiguities
    dict_ = dictionary.copy()
    for k, v in dict_.items():
        if isinstance(v, list):  # there is ambiguity
            if ambiguity == 'skip':
                dictionary.pop(k)
            elif ambiguity == 'error':
                raise ValueError("ambiguities found in dictionary!")
            elif ambiguity == 'prompt':
                # prompt for value
                text = 'Please choose the correct entity to align *{}* with ' \
                       'for the rest of the execution of the recipe:'.format(k)
                val = prompt_select(v, text_before=text)
                if val != -1:
                    dictionary[k] = val
                else:
                    dictionary.pop(k)

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


def _generate_mappng_dict1(df, column, dictionary, base_df, not_found):

    search_col = dictionary['key']
    idx_col = dictionary['value']

    if base_df.set_index(idx_col).index.has_duplicates:
        logging.warning('there are duplicated keys in the base dataframe:')
        m = base_df.set_index(idx_col).index.duplicated()
        logging.warning(base_df.set_index(idx_col).index[m].unique())

    if search_col == idx_col:
        mapping_all = dict([(x, x) for x in base_df[idx_col].values])
    else:
        mapping_all = base_df.set_index(search_col)[idx_col].to_dict()
    return mapping_all


def _generate_mapping_dict2(df, column, dictionary, base_df, not_found):

    mapping = dict()
    no_match = set()

    search_cols = dictionary['key']
    idx_col = dictionary['value']

    assert isinstance(idx_col, str)

    for f in df[column].values:
        bools = []
        for sc in search_cols:
            bools.append(base_df[sc] == f)

        mask = bools[0]
        for m in bools[1:]:
            mask = mask | m
        filtered = base_df[mask]

        if len(filtered) == 1:
            mapping[f] = filtered[idx_col].iloc[0]
        elif len(filtered) > 1:
            logging.warning("multiple match found: "+f)
            mapping[f] = filtered[idx_col].values.tolist()
        else:
            no_match.add(f)

    if len(no_match) > 0:
        logging.warning('no match found: ' + str(no_match))

    if not_found == 'error' and len(no_match) > 0:
        raise ValueError('missing keys in dictionary. please check your input.')
    else:
        return mapping


def _translate_column_df(df, column, target_column, dictionary, base_df,
                         not_found, ambiguity):

    search_cols = dictionary['key']
    if isinstance(search_cols, str):
        mapping = _generate_mappng_dict1(df, column, dictionary, base_df, not_found)
    else:
        if len(search_cols) == 1:
            dictionary['key'] = search_cols[0]
            mapping = _generate_mappng_dict1(df, column, dictionary, base_df, not_found)
        else:
            mapping = _generate_mapping_dict2(df, column, dictionary, base_df, not_found)

    return _translate_column_inline(df, column, target_column, mapping, not_found, ambiguity)


def translate_column(df, column, dictionary_type, dictionary,
                     target_column=None, base_df=None, not_found='drop', ambiguity='prompt'):
    """change values in a column base on a mapping dictionary.

    The dictionary can be provided as a python dictionary, pandas dataframe or read from file.

    Note
    ----
    When translating with a base DataFrame, if ambiguity is found in the data, for example,
    a dataset with entity-id `congo`, to align to a dataset with `cod` ( Democratic Republic
    of the Congo ) and `cog` ( Republic of the Congo ), the function will ask for user input
    to choose which one or to skip it.

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
        The column to store translated resluts. If this is None, then the one set with `column`
        will be replaced.
    `base_df` : `DataFrame`, optional
        When `dictionary_type` is `dataframe`, this option should be set
    `not_found` : `str`
        What to do if key in the dictionary is not found in the dataframe to be translated.
        avaliable options are `drop`, `error`, `include`
    `ambiguity` : `str`
        What to do when there is ambiguities in the dictionary. avaliable options are `prompt`,
        `skip`, `error`

    Examples
    --------
    >>> df = pd.DataFrame([['geo', 'Geographical places'], ['time', 'Year']], columns=['concept', 'name'])
    >>> df
      concept                 name
    0     geo  Geographical places
    1    time                 Year
    >>> translate_column(df, 'concept', 'inline', {'geo': 'country', 'time': 'year'})
       concept                 name
    0  country  Geographical places
    1     year                 Year

    >>> base_df = pd.DataFrame([['geo', 'country'], ['time', 'year']], columns=['concept', 'alternative_name'])
    >>> base_df
      concept alternative_name
    0     geo          country
    1    time             year
    >>> translate_column(df, 'concept', 'dataframe',
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

    if ambiguity not in ['skip', 'error', 'prompt']:
        raise ValueError("ambiguity should be one of 'skip', 'error', 'prompt'")

    if dictionary_type == 'dataframe' and base_df is None:
        raise ValueError("please specify base_df when dictionary type is 'dataframe'")

    if not target_column:
        target_column = column

    if dictionary_type == 'inline':
        df_new = _translate_column_inline(df, column, target_column, dictionary,
                                          not_found, ambiguity)
    if dictionary_type == 'file':
        with open(dictionary) as f:
            d = json.load(f)
        df_new = _translate_column_inline(df, column, target_column, d,
                                          not_found, ambiguity)
    if dictionary_type == 'dataframe':
        assert 'key' in dictionary.keys() and 'value' in dictionary.keys()
        df_new = _translate_column_df(df, column, target_column, dictionary, base_df,
                                      not_found, ambiguity)

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


def trend_bridge(old_data: pd.Series, new_data: pd.Series, bridge_length: int) -> pd.Series:
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
    bridge_end = new_data.index[0]
    bridge_start = bridge_end - bridge_length

    assert not pd.isnull(old_data.ix[bridge_start]), 'no data for bridge start'

    bridge_height = new_data.ix[bridge_end] - old_data.ix[bridge_end]
    fraction = bridge_height / bridge_length

    bridge_data = old_data.copy()

    for i, row in bridge_data.ix[bridge_start:bridge_end].iteritems():
        if i == bridge_end:
            break
        bridge_data.ix[i:bridge_end] = bridge_data.ix[i:bridge_end] + fraction

    # combine old/new/bridged data
    result =  pd.concat([bridge_data.ix[:bridge_end], new_data.iloc[1:]])
    return result


def extract_concepts(dfs, base=None, join='full_outer'):
    """extract concepts from a list of dataframes.

    Parameters
    ----------
    dfs : list[DataFrame]
        a list of dataframes to be extracted

    Keyword Args
    ------------
    base : DataFrame
        the base concept table to join
    join : {'full_outer', 'ingredients_outer'}
        how to join the `base` dataframe. ``full_outer`` means union of the base and extracted,
        ``ingredients_outer`` means only keep concepts in extracted

    Return
    ------
    DataFrame
        the result concept table
    """
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
