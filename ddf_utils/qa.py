# -*- coding: utf-8 -*-

"""QA functioins.
"""

import sys
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger('QA')
this = sys.modules[__name__]


def _gen_indicator_key_list(d):
    for k, v in d.items():
        for i in v:
            yield (k, i)


def compare_with_func(dataset1, dataset2, fns=['rval', 'avg_pct_chg'],
                      indicators=None, key=None):
    """compare 2 datasets with functions"""

    indicators1 = [(k, tuple(sorted(v))) for k, v in _gen_indicator_key_list(dataset1.datapoints)]
    indicators2 = [(k, tuple(sorted(v))) for k, v in _gen_indicator_key_list(dataset2.datapoints)]

    # check availability for indicators
    s1 = set(indicators1)
    s2 = set(indicators2)

    diff12 = s1 - s2
    diff21 = s2 - s1

    if len(diff12) > 0:
        msg = ["below indicators are only available in {}".format(dataset1.attrs['name'])]
        for item in diff12:
            msg.append("- {} by {}".format(item[0], ', '.join(item[1])))
        msg.append('')
        logger.warning('\n'.join(msg))
    if len(diff21) > 0:
        msg = ["below indicators are only available in {}".format(dataset2.attrs['name'])]
        for item in diff21:
            msg.append("- {} by {}".format(item[0], ', '.join(item[1])))
        msg.append('')
        logger.warning('\n'.join(msg))

    # construct a dataframe, including all indicators in both dataset.
    result = pd.DataFrame(list(s1.union(s2)), columns=['indicator', 'primary_key'])

    def get_comp_df(indicator, k):
        '''get dataframes from old and new datasets, and combine them into one dataframe'''
        # FIXME: support multiple indicator in one file
        # like the indicators in ddf--sodertorn--stockholm_lan_basomrade
        try:
            i1 = dataset1.get_datapoint_df(indicator, k).compute().set_index(list(k))
        except KeyError:
            raise
        try:
            i2 = dataset2.get_datapoint_df(indicator, k).compute().set_index(list(k))
        except KeyError:
            raise
        # i1 = i1.rename(columns={indicator: 'old'})
        # i2 = i2.rename(columns={indicator: 'new'})
        # comp = pd.concat([i1, i2], axis=1)
        comp = i1.join(i2, lsuffix='_old', rsuffix='_new')

        return comp

    def do_compare(fns, indicator, k):
        try:
            comp_df = get_comp_df(indicator, k)
        except KeyError:
            return [np.nan] * len(fns)

        # return nan if indicator is not number type
        if comp_df.dtypes[indicator+'_old'] == 'object' or comp_df.dtypes[indicator+'_new'] == 'object':
            return [np.nan] * len(fns)

        return [f(comp_df, indicator) if callable(f) else getattr(this, f)(comp_df, indicator)
                for f in fns]

    # only keep indicators we want to compare
    if indicators:
        result = result[result.indicator.isin(indicators)]
    if key:
        result = result[result.primary_key.isin(key)]

    # append new columns before we do calculation
    for f in fns:
        result[f] = np.nan

    result = result.set_index(['indicator', 'primary_key'])
    result = result.sort_index()
    idx = pd.IndexSlice
    for i in result.index:
        result.loc[idx[i[0], [i[1]]], fns] = np.array(do_compare(fns, i[0], i[1]))

    return result.reset_index()


def rval(comp_df, indicator):
    """return r-value between old and new data"""
    old_name = indicator+'_old'
    new_name = indicator+'_new'
    # logger.warning("{}".format(old_name, new_name))
    # logger.warning("{}".format(comp_df.columns))
    return comp_df.corr().ix[old_name, new_name]


def avg_pct_chg(comp_df, indicator):
    """return average precentage changes between old and new data"""
    old_name = indicator+'_old'
    new_name = indicator+'_new'
    res = (comp_df[new_name] - comp_df[old_name]) / comp_df[old_name] * 100
    return res.replace([np.inf, -np.inf], np.nan).mean()


def max_pct_chg(comp_df, indicator):
    """return average precentage changes between old and new data"""
    old_name = indicator+'_old'
    new_name = indicator+'_new'
    res = (comp_df[new_name] - comp_df[old_name]) / comp_df[old_name] * 100
    res = res.replace([np.inf, -np.inf], np.nan)
    return res.max()


def min_pct_chg(comp_df, indicator):
    """return average precentage changes between old and new data"""
    old_name = indicator+'_old'
    new_name = indicator+'_new'
    res = (comp_df[new_name] - comp_df[old_name]) / comp_df[old_name] * 100
    res = res.replace([np.inf, -np.inf], np.nan)
    return res.min()


def max_change_index(comp_df, indicator):
    # FIXME: this function makes all result column type to be object
    # see test cases in test_qa.py
    old_name = indicator+'_old'
    new_name = indicator+'_new'
    diff = (comp_df[new_name] - comp_df[old_name]) / comp_df[old_name] * 100
    diff = diff.replace([np.inf, -np.inf], np.nan)
    diff = abs(diff)
    if len(diff.dropna()) == 0 or diff.max() == 0:
        return ''
    idx = diff[diff == diff.max()].index.values[0]
    return str(idx)
