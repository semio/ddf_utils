# -*- coding: utf-8 -*-

import os.path as osp
import json
import logging
import ddf_utils.datapackage


def load_datapackage_json(path):
    if osp.isdir(path):
        try:
            dp = json.load(open(osp.join(path, 'datapackage.json')))
        except FileNotFoundError:
            logging.warning('datapackage.json not found, will generate one')
            dp = ddf_utils.datapackage.create_datapackage(path, gen_schema=False)
        basedir = path
    else:
        try:
            dp = json.load(open(path))
        except FileNotFoundError:
            logging.warning('datapackage.json not found, will generate one')
            dp = ddf_utils.datapackage.create_datapackage(path, gen_schema=False)
        basedir = osp.dirname(path)

    return basedir, dp
