# -*- coding: utf-8 -*-

import os
import common
from ddf_utils import DDF
import pytest

@pytest.fixture(scope='session')
def ddf():
    ddf = DDF('ddf--gapminder--dummy_companies')
    return ddf


def test_read_datapoints(ddf):
    dps = ddf.get_datapoints()
    assert len(dps) == 3
    dps_1 = ddf.get_datapoint_df('lines_of_code', primaryKey=['company', 'anno'])
    assert dps_1.shape[0] == 4
    dps_2 = ddf.get_datapoint_df('lines_of_code', primaryKey=['company', 'project'])
    assert dps_2.shape[0] == 6
    dps_3 = ddf.get_datapoint_df('lines_of_code', primaryKey=['company', 'anno', 'project'])
    assert dps_3.shape[0] == 8


def test_read_entities(ddf):
    ent_1 = ddf.get_entities()
    assert len(ent_1) == 7
    ent_2 = ddf.get_entities(domain='company')
    assert len(ent_2) == 4


def test_read_concepts(ddf):
    concepts = ddf.get_concepts()
    assert concepts.shape[0] == 17


def test_ddf_dtype(ddf):
    dps = ddf.get_datapoint_df('lines_of_code', primaryKey=['company', 'anno', 'project'])
    print(dps.reset_index().dtypes)
    assert dps.reset_index().dtypes['lines_of_code'] == 'int64'
    assert dps.reset_index().dtypes['anno'] == 'int64'
