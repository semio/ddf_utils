# -*- coding: utf-8 -*-

import os


def test_create_datapackage_1():
    from ddf_utils.model.package import DDFcsv
    from ddf_utils.package import get_datapackage

    dataset_path = os.path.join(os.path.dirname(__file__),
                                'chef/datasets/ddf--gapminder--dummy_companies')

    dp1 = get_datapackage(dataset_path, use_existing=True)
    assert dp1['license'] == 'foo'

    dp2 = get_datapackage(dataset_path, use_existing=False)
    assert 'license' not in dp2.keys()

    dp_ = DDFcsv.from_path(dataset_path)
    dp_.generate_ddf_schema()
    datapackage = dp_.to_dict()
    assert 'ddfSchema' in datapackage.keys()

    d = {
        "primaryKey": [
            "project"
        ],
        "value": None,
        "resources": [
            "ddf--entities--project"
        ]
    }
    assert d in datapackage['ddfSchema']['entities']


def test_create_datapackage_2():
    from ddf_utils.model.package import DDFcsv

    dataset_path = os.path.join(os.path.dirname(__file__),
                                'chef/datasets/ddf--datapackage--testing')

    dp = DDFcsv.from_path(dataset_path)
    dp.generate_ddf_schema()
    datapackage = dp.to_dict()
    assert 'ddfSchema' in datapackage.keys()

    assert len(datapackage['ddfSchema']['datapoints']) == 3


