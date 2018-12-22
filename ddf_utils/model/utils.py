# -*- coding: utf-8 -*-

import os
import os.path as osp
import json
import logging

from functools import partial



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


# helper for sorting the json object
def sort_json(dp):
    """sort json object. dp means datapackage.json"""
    def get_sort_key(d, ks: list):
        res = []
        for k in ks:
            if isinstance(d[k], (list, tuple)):
                for v in d[k]:
                    res.append(v)
            elif d[k] is None:
                res.append('')
            else:
                res.append(d[k])
        return res

    def proc(x, ks):
        return sorted(x, key=partial(get_sort_key, ks=ks))

    if 'resources' in dp.keys():
        dp['resources'] = proc(dp['resources'], ['path'])

    if 'ddfSchema' in dp.keys():
        schema = dp['ddfSchema']
        for t in ['concepts', 'entities', 'datapoints']:
            if t in schema.keys():
                for v in schema[t]:
                    v['resources'] = sorted(v['resources'])
                schema[t] = proc(schema[t], ['value', 'primaryKey'])

        dp['ddfSchema'] = schema

    return dp


def get_ddf_files(path, root=None):
    """yield all csv files which are named following the DDF model standard.

    Parameters
    -----------
    path : `str`
        the path to check
    root : `str`, optional
        if path is relative, append the root to all files.
    """
    info = next(os.walk(path))

    # don't include hidden and lang/etl dir.
    sub_dirs = [
        x for x in info[1] if (not x.startswith('.') and x not in ['lang', 'etl', 'langsplit'])
    ]
    files = list()
    for x in info[2]:
        if x.startswith('ddf--') and x != 'ddf--index.csv' and x.endswith('.csv'):
            files.append(x)
        else:
            logging.warning('skipping file {}'.format(x))

    for f in files:
        if root:
            yield os.path.join(root, f)
        else:
            yield f

    for sd in sub_dirs:
        for p in get_ddf_files(os.path.join(path, sd), root=sd):
            yield p
