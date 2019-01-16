# -*- coding: utf-8 -*-

import os
from ddf_utils.model.package import DDFcsv
import numpy as np
from numpy.testing import assert_array_equal

wd = os.path.dirname(__file__)


def test_compare_func():
    from ddf_utils.qa import compare_with_func
    d1 = DDFcsv.from_path(os.path.join(wd, 'chef/datasets/ddf--bp--energy')).ddf
    d2 = DDFcsv.from_path(os.path.join(wd, 'chef/datasets/ddf--cme')).ddf

    res1 = compare_with_func(d1, d1, fns=['rval', 'avg_pct_chg', 'max_pct_chg'])
    res2 = compare_with_func(d1, d2, fns=['rval', 'avg_pct_chg', 'max_pct_chg'])

    assert_array_equal(res1.columns,
                       ['indicator', 'primary_key', 'rval',
                        'avg_pct_chg', 'max_pct_chg'])
    assert_array_equal(res1.rval.unique(), np.array([1.]))

    assert_array_equal(res1.columns,
                       ['indicator', 'primary_key', 'rval',
                        'avg_pct_chg', 'max_pct_chg'])
    assert_array_equal(res2.rval.unique(), np.array([np.nan]))


def test_compare_func2():
    from ddf_utils.qa import compare_with_func
    d1 = DDFcsv.from_path(os.path.join(wd, 'chef/datasets/ddf--bp--energy')).ddf
    d2 = DDFcsv.from_path(os.path.join(wd, 'chef/datasets/ddf--cme')).ddf

    res1 = compare_with_func(d1, d1, fns=['rval', 'max_change_index'])
    res2 = compare_with_func(d1, d2, fns=['rval', 'max_change_index'])

    assert_array_equal(res1.columns,
                       ['indicator', 'primary_key', 'rval', 'max_change_index'])
    assert_array_equal(res1.rval.unique(), np.array(['1.0']))
