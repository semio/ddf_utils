# -*- coding: utf-8 -*-

import common
import os
from ddf_utils.model.package import Datapackage
import numpy as np
from numpy.testing import assert_array_equal

wd = os.path.dirname(__file__)


def test_compare_func():
    from ddf_utils.qa import compare_with_func
    d1 = Datapackage(os.path.join(wd, 'datasets/ddf--bp--energy')).load()
    d2 = Datapackage(os.path.join(wd, 'datasets/ddf--cme')).load()

    res1 = compare_with_func(d1, d1, fns=['rval', 'min_pct_chg', 'avg_pct_chg', 'max_pct_chg'])
    res2 = compare_with_func(d1, d2, fns=['rval', 'min_pct_chg', 'avg_pct_chg', 'max_pct_chg'])

    assert_array_equal(res1.columns,
                       ['indicator', 'primary_key', 'rval', 'min_pct_chg',
                        'avg_pct_chg', 'max_pct_chg'])
    assert_array_equal(res1.rval.unique(), np.array([1.]))

    assert_array_equal(res1.columns,
                       ['indicator', 'primary_key', 'rval', 'min_pct_chg',
                        'avg_pct_chg', 'max_pct_chg'])
    assert_array_equal(res2.rval.unique(), np.array([np.nan]))


def test_compare_func2():
    from ddf_utils.qa import compare_with_func
    d1 = Datapackage(os.path.join(wd, 'datasets/ddf--bp--energy')).load()
    d2 = Datapackage(os.path.join(wd, 'datasets/ddf--cme')).load()

    res1 = compare_with_func(d1, d1, fns=['rval', 'max_change_index'])
    res2 = compare_with_func(d1, d2, fns=['rval', 'max_change_index'])

    assert_array_equal(res1.columns,
                       ['indicator', 'primary_key', 'rval', 'max_change_index'])
    assert_array_equal(res1.rval.unique(), np.array(['1.0']))
