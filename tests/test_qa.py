# -*- coding: utf-8 -*-

import common
import os
from ddf_utils.model.ddf import Dataset
import numpy as np
from numpy.testing import assert_array_equal

wd = os.path.dirname(__file__)


def test_compare_func():
    from ddf_utils.qa import compare_with_func
    d1 = Dataset.from_ddfcsv(os.path.join(wd, 'datasets/ddf--bp--energy'))
    d2 = Dataset.from_ddfcsv(os.path.join(wd, 'datasets/ddf--cme'))

    res1 = compare_with_func(d1, d1)
    res2 = compare_with_func(d1, d2)

    assert_array_equal(res1.columns,
                       ['indicator', 'primary_key', 'rval', 'avg_pct_chg'])
    assert_array_equal(res1.rval.unique(), np.array([1.]))

    assert_array_equal(res1.columns,
                       ['indicator', 'primary_key', 'rval', 'avg_pct_chg'])
    assert_array_equal(res2.rval.unique(), np.array([np.nan]))
