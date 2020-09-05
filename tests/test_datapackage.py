# -*- coding: utf-8 -*-

import os
import pytest

from ddf_utils.model.package import DDFcsv
from ddf_utils.package import get_datapackage


def test_create_datapackage_1():
    dataset_path = os.path.join(os.path.dirname(__file__),
                                'chef/datasets/ddf--gapminder--dummy_companies')

    dp1 = get_datapackage(dataset_path, use_existing=True)
    assert dp1['license'] == 'foo'

    dp2 = get_datapackage(dataset_path, use_existing=False)
    for k in ['name', 'title', 'author', 'description', 'language', 'license']:
        assert k in dp2.keys()

    dp_ = DDFcsv.from_path(dataset_path)
    dp_.get_ddf_schema(update=True)
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
    dataset_path = os.path.join(os.path.dirname(__file__),
                                'chef/datasets/ddf--datapackage--testing')

    dp = DDFcsv.from_path(dataset_path)
    dp.get_ddf_schema(update=True)
    datapackage = dp.to_dict()
    assert 'ddfSchema' in datapackage.keys()

    assert len(datapackage['ddfSchema']['datapoints']) == 3


def test_create_datapackage_3():

    dataset_path = os.path.join(os.path.dirname(__file__),
                                'chef/datasets/ddf--datapackage--testing_fail')

    with pytest.raises(ValueError):
        get_datapackage(dataset_path, update=True, use_existing=False)
