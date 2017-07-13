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


# helper functions:
# check if a directory is dataset root dir
def is_dataset(path):
    """check if a directory is a dataset directory

    This function checks if ddf--index.csv and datapackage.json exists
    to judge if the dir is a dataset.
    """
    index_path = osp.join(path, 'ddf--index.csv')
    datapackage_path = osp.join(path, 'datapackage.json')
    if osp.exists(index_path) or osp.exists(datapackage_path):
        return True
    else:
        return False
