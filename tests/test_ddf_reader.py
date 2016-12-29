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
