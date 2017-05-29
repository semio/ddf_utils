# -*- coding: utf-8 -*-

import os


def test_create_datapackage():
    from ddf_utils.datapackage import get_datapackage

    dataset_path = os.path.join(os.path.dirname(__file__),
                                'datasets/ddf--gapminder--dummy_companies')

    dp1 = get_datapackage(dataset_path, use_existing=True)
    assert dp1['license'] == 'foo'

    dp2 = get_datapackage(dataset_path, use_existing=False)
    assert 'license' not in dp2.keys()
