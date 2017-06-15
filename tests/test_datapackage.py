# -*- coding: utf-8 -*-

import os


def test_create_datapackage_1():
    from ddf_utils.datapackage import get_datapackage
    from ddf_utils.model.package import Datapackage

    dataset_path = os.path.join(os.path.dirname(__file__),
                                'datasets/ddf--gapminder--dummy_companies')

    dp1 = get_datapackage(dataset_path, use_existing=True)
    assert dp1['license'] == 'foo'

    dp2 = get_datapackage(dataset_path, use_existing=False)
    assert 'license' not in dp2.keys()

    dp_ = Datapackage(dataset_path)
    dp_.generate_ddfschema()
    assert 'ddfSchema' in dp_.datapackage.keys()

    d = {
        "primaryKey": [
            "project"
        ],
        "value": None,
        "resources": [
            "ddf--entities--project"
        ]
    }
    assert d in dp_.datapackage['ddfSchema']['entities']


def test_create_datapackage_2():
    from ddf_utils.model.package import Datapackage

    dataset_path = os.path.join(os.path.dirname(__file__),
                                'datasets/ddf--datapackage--testing')

    dp = Datapackage(dataset_path)
    dp.generate_ddfschema()
    assert 'ddfSchema' in dp.datapackage.keys()

    assert len(dp.datapackage['ddfSchema']['datapoints']) == 3


def test_dataset():
    from ddf_utils.model.ddf import Dataset

    dataset_path = os.path.join(os.path.dirname(__file__),
                                'datasets/ddf--gapminder--dummy_companies')

    ds = Dataset.from_ddfcsv(dataset_path)

    conc = ds.concepts
    ent = ds.entities
    dps = ds.datapoints

    ent_foundation = ds.get_entity('foundation')
    assert 'is--foundation' in ent_foundation.columns
