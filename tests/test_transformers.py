# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


def test_trend_bridge():

    from ddf_utils.transformer import trend_bridge
    from numpy.testing import assert_almost_equal

    time_range1 = pd.date_range('1900', '2016', freq='A')
    time_range2 = pd.date_range('1980', '2016', freq='A')

    np.random.seed(3981)
    roc1 = np.random.randn(len(time_range1)) / 20
    roc2 = np.random.randn(len(time_range2)) / 20

    data1 = (1+roc1).cumprod()
    data1 = pd.Series(data1, index=time_range1)

    data2 = (1 + roc2).cumprod() * data1.ix['1980-12-31', 'old_data'] + 0.15
    data2 = pd.Series(data2, index=time_range2)

    data3 = trend_bridge(data1, data2, 60)

    assert not data3.index.has_duplicates
    assert_almost_equal(data3.ix['1979-12-31'], 0.497, 5)


def test_extract_concept():
    from ddf_utils.transformer import extract_concepts

    df1 = pd.DataFrame([1, 3, 5], columns=['col1'], index=[2000, 2001, 2002])
    df2 = pd.DataFrame([1, 3, 5], columns=['col2'], index=[1990, 1991, 1992])

    res1 = extract_concepts([df1, df2])
    assert 'col1' in res1.concept.values
    assert 'col2' in res1.concept.values

    base_df = pd.DataFrame([['col0', 'string'], ['col4', 'string']], columns=['concept', 'concept_type'])
    res2 = extract_concepts([df1.reset_index(), df2.reset_index()], base=base_df)
    assert 'col0' in res2.concept.values
    assert 'col1' in res2.concept.values


def test_merge_keys():
    from ddf_utils.transformer import merge_keys

    df = pd.DataFrame([['c1', 1992, 1], ['c2', 1992, 2], ['c3', 1992, 3]], columns=['geo', 'time', 'val'])
    di = {'nc': ['c1', 'c2', 'c3']}

    res1 = merge_keys(df.set_index(['geo', 'time']), di)
    assert res1.get_value(('nc', 1992), 'val') == 6

    res2 = merge_keys(df.set_index(['geo', 'time']), di, merged='keep')
    assert res2.get_value(('c1', 1992), 'val') == 1


def test_split_keys():
    from ddf_utils.transformer import split_keys

    df = pd.DataFrame([['n0', 1991, 6], ['c1', 1992, 1], ['c2', 1992, 2], ['c3', 1992, 3]],
                      columns=['geo', 'time', 'val'])
    di = {'n0': ['c1', 'c2', 'c3']}

    res1 = split_keys(df.set_index(['geo', 'time']), 'geo', di)
    assert res1.get_value(('c1', 1991), 'val') == 1
