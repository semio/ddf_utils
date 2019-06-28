# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from ddf_utils.chef.helpers import gen_query, sort_df


def test_gen_query():
    cond = {'$and': {'year': {'$gt': 1990, '$lt': 2000}}}
    cond = {'$not': {'year': {'$ne': 1990, '$lt': 2000}}}
    cond = {'$nor': {'year': {'$ne': 1990, '$lt': 2000}}}
    gen_query(cond)


def test_sort_df():
    df = pd.DataFrame(np.random.rand(100, 3), columns=['A', 'C', 'B'])
    df1 = sort_df(df, 'A')
    assert df1.columns.tolist() == ['A', 'B', 'C']

    df = pd.DataFrame(np.random.rand(100, 4), columns=['D', 'A', 'C', 'B'])
    df2 = sort_df(df, ['D', 'A'], sort_key_columns=False)
    assert df2.columns.tolist() == ['D', 'A', 'B', 'C']

    df3 = sort_df(df, ['D'], custom_column_order={'A': 1, 'C': -1})
    assert df3.columns.tolist() == ['D', 'C', 'B', 'A']
