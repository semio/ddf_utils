# -*- coding: utf-8 -*-

import os.path as osp
import json


def load_datapackage_json(path):
    if osp.isdir(path):
        dp = json.load(open(osp.join(path, 'datapackage.json')))
        basedir = path
    else:
        dp = json.load(open(path))
        basedir = osp.dirname(path)

    return basedir, dp
