# -*- coding: utf-8 -*-

import os
import ddf_utils

modulepath = os.path.dirname(ddf_utils.__file__)
ddf_utils.config.DDF_SEARCH_PATH = os.path.join(modulepath, '../tests/datasets')
