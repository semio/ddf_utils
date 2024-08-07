# -*- coding: utf-8 -*-

import os

import pandas as pd
import numpy as np


def test_trend_bridge():

    from ddf_utils.transformer import trend_bridge
    from numpy.testing import assert_almost_equal

    tr1 = pd.Index(range(0, 5))
    tr2 = pd.Index(range(3, 8))
    s1 = pd.Series([1, 2, 3, 4, 5], index=tr1)
    s2 = pd.Series([6, 7, 8, 9, 10], index=tr2)
    res1 = trend_bridge(s1, s2, 3)
    assert_almost_equal(res1.values.tolist(), [1, 2 + 2/3, 3 + 4/3, 6, 7, 8, 9, 10])

    tr1 = pd.Index(range(0, 5))
    tr2 = pd.Index(range(10, 15))
    s1 = pd.Series([1, 2, 3, 4, 5], index=tr1)
    s2 = pd.Series([6, 7, 8, 9, 10], index=tr2)
    res2 = trend_bridge(s1, s2, 3)
    assert_almost_equal(res2.values.tolist(), list(range(1, 11)))

    tr1 = pd.Index(range(0, 5))
    tr2 = pd.Index(range(3, 8))
    s1 = pd.Series([1, 2, 3, 4, 5], index=tr1)
    s2 = pd.Series([6, 7, 8, 9, 10], index=tr2)
    res3 = trend_bridge(s1, s2, 10)
    assert_almost_equal(res3.values.tolist(), [1.5, 3, 4.5, 6, 7, 8, 9, 10])

    tr1 = [0, 1, 2, 3, 7, 8]
    tr2 = [5, 6, 7, 8, 9, 10]
    s1 = pd.Series(range(len(tr1)), index=tr1)
    s2 = pd.Series(range(len(tr2)), index=tr2)
    res4 = trend_bridge(s1, s2, 3)
    assert res4.index.values.tolist() == [0, 1, 2, 3, 7, 8, 9, 10]
    assert_almost_equal(res4.loc[7], s2.loc[7])

    tr1 = pd.date_range('1990', '1995', freq='YE')
    tr2 = pd.date_range('1994', '2000', freq='YE')
    s1 = pd.Series([1, 2, 3, 4, 5], index=tr1)
    s2 = pd.Series([6, 7, 8, 9, 10, 11], index=tr2)
    trend_bridge(s1, s2, 3)

    tr1 = pd.Index(range(5, 10))
    tr2 = pd.Index(range(3, 8))
    s1 = pd.Series([1, 2, 3, 4, 5], index=tr1)
    s2 = pd.Series([6, 7, 8, 9, 10], index=tr2)
    try:
        trend_bridge(s1, s2, 10)
    except ValueError:
        pass


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
    df2 = df.copy()
    df2['geo'] = df2['geo'].astype('category')
    di = {'nc': ['c1', 'c2', 'c3']}

    res1_1 = merge_keys(df.set_index(['geo', 'time']), di, 'geo')
    res1_2 = merge_keys(df2.set_index(['geo', 'time']), di, 'geo')
    assert not np.any(res1_1.index.duplicated())
    assert not np.any(res1_2.index.duplicated())
    assert res1_1.shape[0] == 1
    assert res1_1.at[('nc', 1992), 'val'] == 6
    assert res1_2.shape[0] == 1
    assert res1_2.at[('nc', 1992), 'val'] == 6
    assert res1_2.index.get_level_values('geo').dtype.name == 'category'

    res2_1 = merge_keys(df.set_index(['geo', 'time']), di, 'geo', merged='keep')
    res2_2 = merge_keys(df2.set_index(['geo', 'time']), di, 'geo', merged='keep')
    assert not np.any(res2_1.index.duplicated())
    assert not np.any(res2_2.index.duplicated())
    assert res2_1.shape[0] == 4
    assert res2_1.at[('c1', 1992), 'val'] == 1
    assert res2_1.at[('nc', 1992), 'val'] == 6
    assert res2_2.shape[0] == 4
    assert res2_2.at[('c1', 1992), 'val'] == 1
    assert res2_2.at[('nc', 1992), 'val'] == 6
    assert res2_2.index.get_level_values('geo').dtype.name == 'category'


def test_split_keys():
    from ddf_utils.transformer import split_keys

    df = pd.DataFrame([['n0', 1991, 6], ['c1', 1992, 1], ['c2', 1992, 2], ['c3', 1992, 3]],
                      columns=['geo', 'time', 'val'])
    df2 = df.copy()
    df2['geo'] = df2['geo'].astype('category')
    di = {'n0': ['c1', 'c2', 'c3']}

    res1_1 = split_keys(df.set_index(['geo', 'time']), 'geo', di)
    res1_2 = split_keys(df2.set_index(['geo', 'time']), 'geo', di)
    assert res1_1.at[('c1', 1991), 'val'] == 1
    assert res1_1.at[('c2', 1991), 'val'] == 2
    assert res1_1.at[('c3', 1991), 'val'] == 3
    assert res1_1.at[('c3', 1992), 'val'] == 3
    assert res1_2.at[('c1', 1991), 'val'] == 1
    assert res1_2.at[('c2', 1991), 'val'] == 2
    assert res1_2.at[('c3', 1991), 'val'] == 3
    assert res1_2.at[('c3', 1992), 'val'] == 3
    assert 'n0' not in res1_1.index.get_level_values('geo')
    assert 'n0' not in res1_2.index.get_level_values('geo')
    assert res1_2.index.get_level_values('geo').dtype.name == 'category'

    res2_1 = split_keys(df.set_index(['geo', 'time']), 'geo', di, splited='keep')
    res2_2 = split_keys(df2.set_index(['geo', 'time']), 'geo', di, splited='keep')
    assert res2_1.at[('c1', 1991), 'val'] == 1
    assert res2_1.at[('c2', 1991), 'val'] == 2
    assert res2_1.at[('c3', 1991), 'val'] == 3
    assert res2_1.at[('c3', 1992), 'val'] == 3
    assert res2_2.at[('c1', 1991), 'val'] == 1
    assert res2_2.at[('c2', 1991), 'val'] == 2
    assert res2_2.at[('c3', 1991), 'val'] == 3
    assert res2_2.at[('c3', 1992), 'val'] == 3
    assert res2_2.at[('n0', 1991), 'val'] == 6
    assert res2_2.at[('n0', 1991), 'val'] == 6
    assert res2_2.index.get_level_values('geo').dtype.name == 'category'


def test_extract_concepts():
    from ddf_utils.transformer import extract_concepts

    geo_data = [{'geo': 'abkh',
                 'is--country': 'TRUE',
                 'name': 'Abkhazia',
                 'world_4region': 'europe',
                 'world_6region': 'europe_central_asia'},
                {'geo': 'afg',
                 'is--country': 'TRUE',
                 'name': 'Afghanistan',
                 'world_4region': 'asia',
                 'world_6region': 'south_asia'}]

    datapoint_data = [{'geo': 'abkh',
                       'time': 1999,
                       'indicator': 10},
                      {'geo': 'afg',
                       'time': 2000,
                       'indicator': 20}]

    df1 = pd.DataFrame.from_records(geo_data)
    df2 = pd.DataFrame.from_records(datapoint_data)

    concepts = extract_concepts([df1, df2]).set_index('concept')
    assert 'country' in concepts.index
    assert 'time' in concepts.index
    assert concepts.loc['country', 'concept_type'] == 'entity_set'


def test_translate_column():
    # from `translate_column`'s heredoc
    from ddf_utils.transformer import translate_column

    df = pd.DataFrame([['geo', 'Geographical places'], ['time', 'Year']],
                      columns=['concept', 'name'])

    r1 = translate_column(df, 'concept', 'inline', {'geo': 'country', 'time': 'year'})

    base_df = pd.DataFrame([['geo', 'country'], ['time', 'year']],
                           columns=['concept', 'alternative_name'])
    r2 = translate_column(df, 'concept', 'dataframe',
                          {'key': 'concept', 'value': 'alternative_name'},
                          target_column='new_name', base_df=base_df)

    df2 = pd.DataFrame([['China', 1], ['United State', 2]], columns=['geo', 'value'])
    base_df2 = pd.DataFrame([['chn', 'China', 'PRC'],
                             ['usa', 'USA', 'United State']],
                            columns=['geo', 'alt1', 'alt2'])
    r3 = translate_column(df2, 'geo', 'dataframe',
                          {'key': ['alt1', 'alt2'], 'value': 'geo'},
                          target_column='new_geo', base_df=base_df2)
    print(r3)


def test_translate_header():
    from ddf_utils.transformer import translate_header

    wd = os.path.dirname(__file__)

    df = pd.DataFrame([[0, 1, 2]], columns=['imr_lower', 'imr_median', 'no_name'])
    translate_header(df, {'no_name': 'yes_name'})
    translate_header(df, os.path.join(wd, 'chef/translation_dictionaries/indicators_cme_to_sg.json'), dictionary_type='file')

    try:
        translate_header(df, {}, dictionary_type='something')
    except ValueError:
        pass
