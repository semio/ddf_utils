# -*- coding: utf-8 -*-

import os


wd = os.path.dirname(__file__)


def test_build_dictionary():
    from ddf_utils.chef.api import Chef
    from ddf_utils.chef.helpers import build_dictionary

    d = {'China': 'chn', 'USA': 'usa'}
    c = Chef()
    assert build_dictionary(c, d) == d

    d2 = {
        "imr_median": "infant_mortality_median",
        "imr_upper": "imr_lower"
    }

    dfp = os.path.join(wd, 'chef', 'translation_dictionaries')
    fp = 'indicators_cme_to_sg.json'
    c.add_config(dictionaries_dir=dfp)
    assert build_dictionary(c, fp) == d2


def test_retry():
    import time
    from ddf_utils.factory.common import retry
    from numpy.testing import assert_almost_equal

    @retry(times=4)
    def test():
        raise NotImplementedError

    t0 = time.time()
    try:
        test()
    except NotImplementedError:
        pass
    t1 = time.time()
    print('Took', t1 - t0, 'seconds')
    assert_almost_equal(t1 - t0, 3, 1)  # 0.5 + 1 + 1.5 = 3
