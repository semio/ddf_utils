# -*- coding: utf-8 -*-

import os.path as osp

from functools import partial

# TODO: maybe move this module out of model/


# return absolute path from any path string
def absolute_path(path: str) -> str:
    if not osp.isabs(path):
        return osp.abspath(osp.expanduser(path))
    else:
        return path


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


