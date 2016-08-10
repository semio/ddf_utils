# -*- coding: utf-8 -*-

import pandas as pd
import os


SEARCH_PATH = ''

def ddf_concepts(ddf_id):
    """return all concepts"""
    path = _get_ddf_path(ddf_id)

    if os.path.exists(os.path.join(path, 'ddf--concepts.csv')):
        df = pd.read_csv(os.path.join(path, 'ddf--concepts.csv'))
        return {'concepts': df}
    else:
        df1 = pd.read_csv(os.path.join(path, 'ddf--concepts--discrete.csv'))
        df2 = pd.read_csv(os.path.join(path, 'ddf--concepts--continuous.csv'))
        return {'continuous': df2, 'discrete': df1}


def ddf_entities(ddf_id):
    """return all entities"""
    index = _get_index(ddf_id)
    path = _get_ddf_path(ddf_id)

    files = index[index['file'].str.contains('entities')]['file']

    res = {}

    for f in files.values:
        # print(f)
        ent = f[:-4].split('--')[-1]
        df = pd.read_csv(os.path.join(path, f))
        res[ent] = df

    return res


def ddf_datapoints(ddf_id):
    """return all datapoints

    in case of there are multiple files for one concept (with different keys)
    this function will return a dictionary in the form of {(concept, keys): dataframe}
    """
    index = _get_index(ddf_id)
    path = _get_ddf_path(ddf_id)

    index = index.set_index('value')
    files = index[index['file'].str.contains('datapoints')][['key', 'file']].drop_duplicates()

    res = {}

    for c, f in files.iterrows():
        fn = f['file']
        df = pd.read_csv(os.path.join(path, fn))
        res[(c, f['key'])] = df

    return res


def ddf_datapoint(ddf_id, concept, key=None):
    """return one datapoint"""
    index = _get_index(ddf_id)
    path = _get_ddf_path(ddf_id)

    index = index.set_index('value')

    f = index.ix[concept][['key', 'file']]

    if len(f) == 0:
        raise KeyError("concept not found: " + concept)

    if len(f) > 1:
        if not key:
            print("WARNING: found multiple files for concept: " + concept)
            print("using the first one in the index")
            fn = f['file'].values[0]
        else:
            fn = f.loc[f['key'] == key, 'file'].values[0]
    else:
        fn = f['file'].values[0]

    return pd.read_csv(os.path.join(path, fn))


def _get_ddf_path(ddf_id):
    global SEARCH_PATH

    if isinstance(SEARCH_PATH, str):
        SEARCH_PATH = [SEARCH_PATH]

    for p in SEARCH_PATH:
        path = os.path.join(p, ddf_id)
        if os.path.exists(path):
            return path
    else:
        raise ValueError('data set not found: {}'.format(ddf_id))


def _get_index(ddf_id):
    """
    return the index file of ddf_id.
    if the file don't exists, create one
    """
    ddf_path = _get_ddf_path(ddf_id)
    index_path = os.path.join(ddf_path, 'ddf--index.csv')

    if os.path.exists(index_path):
        return pd.read_csv(index_path)
    else:
        from . index import create_index_file
        print("no index file, creating one...")
        return create_index_file(ddf_path)


def ddf_index_cols(ddf_id, concept):
    """return the index columns for given concept
    """
    index = _get_index(ddf_id)
    filtered = index[index['value'] == concept]
    assert len(filtered) == 1
    keys = filtered['key'].values[0]
    return keys.split(',')
