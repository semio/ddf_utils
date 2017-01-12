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


