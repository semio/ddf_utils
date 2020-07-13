# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from ddf_utils.chef.helpers import gen_query, sort_df


def test_gen_query():
    cond = {'year': {'$gt': 2000}}
    assert gen_query(cond) == "`year` > '2000'"

    cond = {'year': {'$gte': '2000'}}
    assert gen_query(cond) == "`year` >= '2000'"

    cond = {'indicator': {'$lt': 2000}}
    assert gen_query(cond) == "`indicator` < 2000"

    cond = {'$and': [ {'country': {'$in': ['georgia']}}, {'is--country': True}]}
    assert gen_query(cond) == "(`country` in ['georgia'] and `is--country` == True)"

    cond = {'$or': [ {'country': 'geo'}, {'$and': [ {'name': 'Georgia' }, {'is--country': True}]}]}
    assert gen_query(cond) == "(`country` == 'geo' or (`name` == 'Georgia' and `is--country` == True))"

    cond = {'year': {'$gt': 2000}, 'country': {'$eq': 'swe'}}
    assert gen_query(cond) == "(`year` > '2000' and `country` == 'swe')"

    cond = {'$not': {'$and':
                     [{'year': {'$gt': 2000}}, {'year': {'$lt': 2010}}],
                     'country': {'$eq': 'swe'}}}
    assert gen_query(cond) == "~(((`year` > '2000' and `year` < '2010') and `country` == 'swe'))"

    cond = {'$nor': [ {'country': 'geo'}, {'$and': [ {'name': 'Georgia' }, {'is--country': True}]}]}
    assert gen_query(cond) == "~(`country` == 'geo') and ~((`name` == 'Georgia' and `is--country` == True))"


def test_sort_df():
    df = pd.DataFrame(np.random.rand(100, 3), columns=['A', 'C', 'B'])
    df1 = sort_df(df, 'A')
    assert df1.columns.tolist() == ['A', 'B', 'C']

    df = pd.DataFrame(np.random.rand(100, 4), columns=['D', 'A', 'C', 'B'])
    df2 = sort_df(df, ['D', 'A'], sort_key_columns=False)
    assert df2.columns.tolist() == ['D', 'A', 'B', 'C']

    df3 = sort_df(df, ['D'], custom_column_order={'A': 1, 'B': -1})
    assert df3.columns.tolist() == ['D', 'A', 'C', 'B']
