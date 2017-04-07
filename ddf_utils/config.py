# -*- coding: utf-8 -*-

"""global variables for ddf_utils"""

import os

DDF_SEARCH_PATH = os.getenv('DDF_SEARCH_PATH')

if not DDF_SEARCH_PATH:
    DDF_SEARCH_PATH = './'

DICT_PATH = None
DEBUG_ALL = False
DEBUG_OUTPUT_PATH = None
