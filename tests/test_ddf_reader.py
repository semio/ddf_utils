# -*- coding: utf-8 -*-

import os
import common
from ddf_utils import DDF
import pytest

@pytest.fixture(scope='session')
def ddf():
    ddf = DDF('ddf--ilo--kilm_employment_sector')
    return ddf

def test_ddf(ddf):
    dps = ddf.get_datapoints()
    entities = ddf.get_entities()

    assert len(dps) == 2
    assert len(entities) == 2

def test_read_entities():
    ddf = DDF('ddf--gapminder--geo_entity_domain')
    ent_1 = ddf.get_entities()
    assert len(ent_1) == 8
    ent_2 = ddf.get_entities(domain='geo')
    assert len(ent_2) == 8


def test_ddf_dtype(ddf):
    dps = ddf.get_datapoint_df('agriculture_percentage')
    print(dps.reset_index().dtypes)
    assert dps.reset_index().dtypes['agriculture_percentage'] == 'float64'
    assert dps.reset_index().dtypes['year'] == 'int64'
