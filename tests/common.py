# -*- coding: utf-8 -*-

import os
import ddf_utils

wd = os.path.dirname(__file__)
ddf_utils.config.DDF_SEARCH_PATH = os.path.join(wd, 'datasets/')
