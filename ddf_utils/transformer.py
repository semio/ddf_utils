# -*- coding: utf-8 -*-

"""functions for common tasks on ddf datasets"""

import pandas as pd
import dask.dataframe as dd
import numpy as np
import json
import logging

from functools import partial
from ddf_utils.chef.helpers import prompt_select


logger = logging.getLogger(__name__)


def _check_ambiguities(dictionary, ambiguity):
    # check for ambiguities
    dict_ = dictionary.copy()
    for k, v in dictionary.items():
        if isinstance(v, list):  # there is ambiguity
            if ambiguity == 'skip':
                dict_.pop(k)
            elif ambiguity == 'error':
                raise ValueError("ambiguities found in dict_!")
            elif ambiguity == 'prompt':
                # prompt for value
                text = 'Please choose the correct entity to align *{}* with ' \
                       'for the rest of the execution of the recipe:'.format(k)
                val = prompt_select(v, text_before=text)
                if val != -1:
                    dict_[k] = val
                else:
                    dict_.pop(k)
    return dict_


def __print_not_found(series, dictionary):
    nf = set()
    if type(series) is dd.Series:
        uniques = series.unique().compute()
    else:
        uniques = series.unique()
    for v in uniques:
        if v not in dictionary.keys():
            nf.add(v)
    if len(nf) > 0:
        logger.warning('key not found:')
        logger.warning(list(nf))


def __process_val(v, dictionary=None):
    # check if a value is in the dictionary, if not, return nan
    # and mark down the not found values
    #
    # There is an issue in dask, see dask issue #4127
    # if v == 'foo':
    #     import ipdb; ipdb.set_trace()
    if v in dictionary.keys():
        return dictionary[v]
    else:
        return np.nan


def _translate_column_inline(df, column, target_column, dictionary,
                             not_found, ambiguity, ignore_case=False):
    df_new = df.copy()

    if ignore_case:
        df_new[column] = df_new[column].map(
            lambda x: str(x).lower() if x is not None else x)

    dictionary = _check_ambiguities(dictionary, ambiguity)

    # TODO: refactor this block
    # handling values not found in the dictionary
    if not_found == 'drop':
        __print_not_found(df_new[column], dictionary)
        process_val = partial(__process_val, dictionary=dictionary)
        df_new[target_column] = df_new[column].map(process_val)
        df_new = df_new.dropna(subset=[target_column])
    if not_found == 'error':
        df_new[target_column] = df_new[column].map(
            lambda x: dictionary[x])
    if not_found == 'include':
        if target_column not in df_new.columns:
            # create a new column: if a key not in the mappings, return NaN
            df_new[target_column] = df_new[column].map(
                lambda x: dictionary[x] if x in dictionary.keys() else np.nan)
        else:
            # update existing column: if a key not in the mappings, use the original val
            df_new['__new_col'] = df_new[column].map(
                lambda x: dictionary[x] if x in dictionary.keys() else np.nan)
            df_new['__new_col'] = df_new['__new_col'].fillna(df_new[target_column])
            df_new[target_column] = df_new['__new_col']
            df_new = df_new.drop('__new_col', axis=1)

    return df_new


def _generate_mapping_dict1(df, column, dictionary, base_df, not_found):

    search_col = dictionary['key']
    idx_col = dictionary['value']

    # Note: don't find mappings for n/a
    base_df_ = base_df.dropna(subset=[idx_col]).dropna(subset=[search_col])

    if base_df_.set_index(idx_col).index.has_duplicates:
        logger.warning('there are duplicated keys in the base dataframe:')
        m = base_df_.set_index(idx_col).index.duplicated()
        logger.warning(base_df_.set_index(idx_col).index[m].unique())

    if search_col == idx_col:
        mapping_all = dict([(x, x) for x in base_df_[idx_col].values])
    else:
        mapping_all = base_df_.set_index(search_col)[idx_col].to_dict()
    # add logger for no match
    no_match = set()

    # need to compute the actual dataframe now if it's a dask dataframe.
    # otherwise it will raise error.
    if isinstance(df, dd.DataFrame):
        uvs = df[column].compute().unique()
    else:
        uvs = df[column].unique()
    for v in uvs:
        if v not in mapping_all.keys():
            no_match.add(v)
    if len(no_match) > 0:
        logger.warning('no match found: ' + str(no_match))
    return mapping_all


def _generate_mapping_dict2(df, column, dictionary, base_df, not_found, ignore_case):

    mapping = dict()
    no_match = set()

    search_cols = dictionary['key']
    idx_col = dictionary['value']

    assert isinstance(idx_col, str)

    for f in df[column].unique():
        if pd.isnull(f):
            logger.warning('skipping n/a values in column {}'.format(column))
            continue
        bools = []
        for sc in search_cols:
            if ignore_case:
                bools.append(base_df[sc].str.lower() == f.lower())
            else:
                bools.append(base_df[sc] == f)

        mask = bools[0]
        for m in bools[1:]:
            mask = mask | m
        filtered = base_df[mask]

        if len(filtered) == 0 or np.all(pd.isnull(filtered[idx_col])):
            no_match.add(f)
            continue

        if len(filtered) == 1:
            mapping[f] = filtered[idx_col].iloc[0]
        elif len(filtered) > 1:
            logger.warning("multiple match found: "+f)
            mapping[f] = filtered[idx_col].values.tolist()

    if len(no_match) > 0:
        logger.warning('no match found: ' + str(no_match))

    if not_found == 'error' and len(no_match) > 0:
        raise ValueError('missing keys in dictionary. please check your input.')
    else:
        return mapping


def _translate_column_df(df, column, target_column, dictionary, base_df,
                         not_found, ambiguity, ignore_case):

    search_cols = dictionary['key']
    if isinstance(search_cols, str):
        mapping = _generate_mapping_dict1(df, column, dictionary, base_df, not_found)
    else:
        if len(search_cols) == 1:
            dictionary['key'] = search_cols[0]
            mapping = _generate_mapping_dict1(df, column, dictionary, base_df, not_found,
                                              ignore_case)
        else:
            mapping = _generate_mapping_dict2(df, column, dictionary, base_df, not_found,
                                              ignore_case)

    return _translate_column_inline(df, column, target_column, mapping, not_found, ambiguity)


def translate_column(df, column, dictionary_type, dictionary,
                     target_column=None, base_df=None, not_found='drop', ambiguity='prompt',
                     ignore_case=False):
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
                                          not_found, ambiguity, ignore_case)
    elif dictionary_type == 'file':
        with open(dictionary) as f:
            d = json.load(f)
        df_new = _translate_column_inline(df, column, target_column, d,
                                          not_found, ambiguity)
    elif dictionary_type == 'dataframe':
        assert 'key' in dictionary.keys() and 'value' in dictionary.keys()
        df_new = _translate_column_df(df, column, target_column, dictionary, base_df,
                                      not_found, ambiguity, ignore_case)
    else:
        raise ValueError("dictionary_type not supported: ()".format(dictionary_type))

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
        with open(dictionary, 'r') as f:
            dictionary = json.load(f)
        return df.rename(columns=dictionary)
    else:
        raise ValueError('dictionary not supported: '+dictionary_type)


def trend_bridge(old_ser: pd.Series, new_ser: pd.Series, bridge_length: int) -> pd.Series:
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
    old_data = old_ser.sort_index()
    new_data = new_ser.sort_index()

    if old_data.empty:
        return new_data
    if new_data.empty:
        return old_data

    bridge_end = new_data.index[0]

    if old_data.index[0] > bridge_end:  # not bridging in this case
        return new_data

    if old_data.index[-1] < bridge_end:
        return pd.concat([old_data, new_data], sort=False)

    # recifying bridge_end, if old_data do not have data at this point
    if bridge_end not in old_data.index:
        intersection = old_data.loc[old_data.index.isin(new_data.index)]
        if intersection.empty:
            raise ValueError("can't bridge because there is no intersection time point")
        bridge_end = intersection.index[0]

    # now recify bridge_length/bridge_start, because in some case old_data just don't have enough data
    s1 = old_data.loc[:bridge_end]
    if s1.shape[0] < bridge_length:
        bridge_length = s1.shape[0]
        bridge_start = old_data.index[0]
    else:
        bridge_start = s1.index.values[-bridge_length:][0]

    bridge_height = new_data.loc[bridge_end] - old_data.loc[bridge_end]
    fraction = bridge_height / bridge_length

    bridge_data = old_data.copy()

    for i in old_data.loc[bridge_start:bridge_end].index:
        bridge_data.loc[i:bridge_end] = bridge_data.loc[i:bridge_end] + fraction

    # combine old/new/bridged data
    result = pd.concat([bridge_data.loc[:bridge_end], new_data.loc[bridge_end:].iloc[1:]], sort=False)
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
            if c.startswith('is--'):
                ent = c[4:]
                new_concepts.add(ent)
                # concept in the is--header should be entity_set
                concepts.loc[c[4:], 'concept_type'] = 'entity_set'
                continue
            if np.issubdtype(df[c].dtype, np.number):
                concepts.loc[c, 'concept_type'] = 'measure'
            else:
                concepts.loc[c, 'concept_type'] = 'string'
    if join == 'ingredients_outer':
        # ingredients_outer join: only keep concepts appears in ingredients
        concepts = concepts.loc[new_concepts]
    return concepts.reset_index()


def merge_keys(df, dictionary, target_column, merged='drop'):
    """merge keys"""
    rename_dict = dict()
    for new_key, val in dictionary.items():
        for old_key in val:
            rename_dict[old_key] = new_key
    # TODO: limit the rename inside target_column
    # after pandas 0.20.0 there will be a level option for df.rename
    df_new = (df.rename(index=rename_dict, level=target_column)
              .groupby(level=list(range(len(df.index.levels))))
              .sum())

    if merged == 'keep':
        df_new = pd.concat([df_new, df])
    elif merged != 'drop':
        raise ValueError('only "drop", "keep" is allowed')

    if df.index.get_level_values(target_column).dtype.name == 'category':
        df_new = df_new.reset_index()
        df_new[target_column] = df_new[target_column].astype('category')
        df_new = df_new.set_index(df.index.names)

    return df_new


def split_keys(df, target_column, dictionary, splited='drop'):
    """split entities"""
    keys = df.index.names
    df_ = df.reset_index()

    ratio = dict()

    for k, v in dictionary.items():
        # dictionary format:
        # {entity_to_split: [sub_entity_1, ...]}
        # the split ratio will be calculated from first valid values of sub entities.
        # so it assumes existence of sub entities
        # TODO: maybe make it work even sub entities not exists.
        before_spl = list()
        for spl in v:
            if spl not in df_[target_column].values:
                raise ValueError('entity not in data: ' + spl)
            tdf = df_[df_[target_column] == spl].set_index(keys).sort_index()
            last = pd.DataFrame(tdf.loc[tdf.index[0], tdf.columns]).T
            last.index.names = keys
            logger.debug("using {} for first valid index".format(tdf.index[0]))
            last = last.reset_index()
            for key in keys:
                if key != target_column:
                    last = last.drop(key, axis=1)
            before_spl.append(last.set_index(target_column))
        before_spl = pd.concat(before_spl)
        total = before_spl.sum()
        ptc = before_spl / total
        ratio[k] = ptc.to_dict()

    logger.debug(ratio)
    # the ratio format will be:
    # ratio = {
    #     'entity_to_split': {
    #         'concept_1': {
    #             'sub_entity_1': r11,
    #             'sub_entity_2': r12,
    #             ...
    #         },
    #         'concept_2': {
    #             'sub_entity_1': r21,
    #             'sub_entity_2': r22,
    #             ...
    #         }
    #     }
    # }

    to_concat = []
    for k, v in ratio.items():
        t = df_[df_[target_column] == k].copy()
        for x, y in v.items():
            for g, r in y.items():
                t_ = t.copy()
                t_[x] = t_[x] * r
                t_[target_column] = g
                to_concat.append(t_.set_index(keys))

    to_concat.append(df)
    final = pd.concat(to_concat)
    if splited == 'drop':
        final = final[~final.index.get_level_values(target_column).isin(dictionary.keys())]
    elif splited != 'keep':
        raise ValueError('only support drop == "drop" and "keep".')

    if df.index.get_level_values(target_column).dtype.name == 'category':
        final = final.reset_index()
        final[target_column] = final[target_column].astype('category')
        final = final.set_index(df.index.names)

    return final.sort_index()
