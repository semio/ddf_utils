# -*- coding: utf-8 -*-

"""QA functioins.
"""

import sys
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger('QA')
this = sys.modules[__name__]


def _gen_indicator_key_list(ddf):
    for indicator, key_data_pair in ddf.datapoints.items():
        for key in key_data_pair.keys():
            yield(indicator, key)


def compare_with_func(dataset1, dataset2, fns=None,
                      indicators=None, key=None, **kwargs):
    """compare 2 datasets with functions"""

    if not fns:
        fns = ['rmse', 'nrmse']

    indicators1 = list(_gen_indicator_key_list(dataset1))
    indicators2 = list(_gen_indicator_key_list(dataset2))

    # check availability for indicators
    s1 = set(indicators1)
    s2 = set(indicators2)

    diff12 = s1 - s2
    diff21 = s2 - s1

    if len(diff12) > 0:
        msg = ["below indicators are only available in {}".format(dataset1.props['name'])]
        for item in diff12:
            msg.append("- {} by {}".format(item[0], ', '.join(item[1])))
        msg.append('')
        logger.warning('\n'.join(msg))
    if len(diff21) > 0:
        msg = ["below indicators are only available in {}".format(dataset2.props['name'])]
        for item in diff21:
            msg.append("- {} by {}".format(item[0], ', '.join(item[1])))
        msg.append('')
        logger.warning('\n'.join(msg))

    # construct a dataframe, including all indicators in both dataset.
    result = pd.DataFrame(list(s1.union(s2)), columns=['indicator', 'primary_key'])

    def get_comp_df(indicator, k):
        """get dataframes from old and new datasets, and combine them into one dataframe"""
        # FIXME: support multiple indicator in one file
        # like the indicators in ddf--sodertorn--stockholm_lan_basomrade
        try:
            i1 = dataset1.get_datapoints(indicator, k).data.compute().set_index(list(k))
        except KeyError:
            raise
        try:
            i2 = dataset2.get_datapoints(indicator, k).data.compute().set_index(list(k))
        except KeyError:
            raise
        # i1 = i1.rename(columns={indicator: 'old'})
        # i2 = i2.rename(columns={indicator: 'new'})
        # comp = pd.concat([i1, i2], axis=1)
        comp = i1.join(i2, how='outer', lsuffix='_old', rsuffix='_new')

        return comp

    def do_compare(fns, indicator, k):
        try:
            comp_df = get_comp_df(indicator, k)
        except KeyError:
            return [np.nan] * len(fns)

        # return nan if indicator is not number type
        if comp_df.dtypes[indicator+'_old'] == 'object' or comp_df.dtypes[indicator+'_new'] == 'object':
            return [np.nan] * len(fns)

        return [f(comp_df, indicator, **kwargs)
                if callable(f)
                else getattr(this, f)(comp_df, indicator, **kwargs)
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


def rval(comp_df, indicator, on='geo'):
    """return r-value between old and new data"""
    old_name = indicator+'_old'
    new_name = indicator+'_new'
    # logger.warning("{}".format(old_name, new_name))
    # logger.warning("{}".format(comp_df.columns))

    def f(df):
        df_ = df - df.shift(1)
        r = df_.corr().loc[old_name, new_name]
        return r

    level = comp_df.index.names.index(on)
    res = comp_df.groupby(level=level).apply(f)

    return res.mean()


def avg_pct_chg(comp_df, indicator, on='geo'):
    """return average precentage changes between old and new data"""
    old_name = indicator+'_old'
    new_name = indicator+'_new'
    level = comp_df.index.names.index(on)

    def f(df):
        new = df[new_name]
        old = df[old_name]
        chg = (new - old) / old * 100

        return chg.replace([np.inf, -np.inf], np.nan).mean()

    res = comp_df.groupby(level=level).apply(f)

    return res.abs().mean()


def max_pct_chg(comp_df, indicator, **kwargs):
    """return average precentage changes between old and new data"""
    old_name = indicator+'_old'
    new_name = indicator+'_new'
    res = (comp_df[new_name] - comp_df[old_name]) / comp_df[old_name] * 100
    res = res.replace([np.inf, -np.inf], np.nan)
    return res.abs().max()


# def min_pct_chg(comp_df, indicator):
#     """return average precentage changes between old and new data"""
#     old_name = indicator+'_old'
#     new_name = indicator+'_new'
#     res = (comp_df[new_name] - comp_df[old_name]) / comp_df[old_name] * 100
#     res = res.replace([np.inf, -np.inf], np.nan)
#     return res.min()


def max_change_index(comp_df, indicator, **kwargs):
    old_name = indicator+'_old'
    new_name = indicator+'_new'
    diff = (comp_df[new_name] - comp_df[old_name]) / comp_df[old_name] * 100
    diff = diff.replace([np.inf, -np.inf], np.nan)
    diff = abs(diff)
    if len(diff.dropna()) == 0 or diff.max() == 0:
        return ''
    idx = diff[diff == diff.max()].index.values[0]
    return str(idx)


def rmse(comp_df, indicator, **kwargs):
    old_name = indicator+'_old'
    new_name = indicator+'_new'
    diff_2 = np.power(comp_df[new_name] - comp_df[old_name], 2)
    rmse_val = np.sqrt(np.sum(diff_2) / len(diff_2))

    return rmse_val


def nrmse(comp_df, indicator, **kwargs):
    old_name = indicator+'_old'
    new_name = indicator+'_new'

    rmse_val = rmse(comp_df, indicator)
    nrmse_val = rmse_val / (comp_df[old_name].max() - comp_df[new_name].min())

    return nrmse_val


def new_datapoints(comp_df, indicator, **kwargs):
    old_name = indicator+'_old'
    new_name = indicator+'_new'

    count = comp_df[new_name].isnull().sum()
    return count


def dropped_datapoints(comp_df, indicator, **kwargs):
    old_name = indicator+'_old'
    new_name = indicator+'_new'

    count = comp_df[old_name].isnull().sum()
    return count
