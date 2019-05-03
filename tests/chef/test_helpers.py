# -*- coding: utf-8 -*-

from ddf_utils.chef.helpers import gen_query


def test_gen_query():
    cond = {'$and': {'year': {'$gt': 1990, '$lt': 2000}}}
    cond = {'$not': {'year': {'$ne': 1990, '$lt': 2000}}}
    cond = {'$nor': {'year': {'$ne': 1990, '$lt': 2000}}}
    gen_query(cond)
