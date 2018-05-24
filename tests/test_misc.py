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
        "imr_lower": "infant_mortality_lower",
        "imr_median": "infant_mortality_median",
        "imr_upper": "infant_mortality_upper"
    }

    dfp = os.path.join(wd, 'chef', 'translation_dictionaries')
    fp = 'indicators_cme_to_sg.json'
    c.add_config(dictionaries_dir=dfp)
    assert build_dictionary(c, fp) == d2
