# -*- coding: utf-8 -*-
"""recipe cooking"""

import os
import pandas as pd
import json
import yaml
import re


SEARCH_PATH = ''
DICT_PATH = ''


class Ingredient(object):
    """
    ingredient class: represents an ingredient object in recipe file.
    """
    def __init__(self, ingred_id, ddf_id, key, values, row_filter=None, data=None):
        self.ingred_id = ingred_id
        self.ddf_id = ddf_id
        self.key = key
        self.values = values
        self.row_filter = row_filter
        self.data = data
        # self.index = _get_index(ddf_id)

    @classmethod
    def from_dict(cls, data):
        ingred_id = data['id']
        ddf_id = data['dataset']
        key = data['key']
        values = data['value']
        if 'filter' in data.keys():
            row_filter = data['filter']
        else:
            row_filter = None

        return cls(ingred_id, ddf_id, key, values, row_filter)

    @property
    def ddf_path(self):
        return _get_ddf_path(self.ddf_id)

    @property
    def dtype(self):
        if self.key == 'concept':
            return 'concept'
        elif isinstance(self.key, list):
            return 'entity'
        else:
            return 'datapoint'

    def __repr__(self):
        lines = []
        lines.append('Ingredient: ' + self.ingred_id)
        lines.append('dataset: '+self.ddf_id)
        lines.append('key: '+str(self.key))
        lines.append('values: '+str(self.values))
        if self.row_filter:
            lines.append('row_filter: Yes')
        else:
            lines.append('row_filter: No')

        return '\n'.join(lines)

    def key_to_list(self):
        # TODO: if key == "*"?
        if self.dtype == "datapoint":
            return self.key.split(',')
        else:
            raise ValueError("only datapoint should call this method")

    def filter_key_value(self):

        index = _get_index(self.ddf_id)
        key = self.key

        if isinstance(self.values, list):
            values = self.values
            if isinstance(self.key, list):
                return index[(index["key"].isin(key)) & (index['value'].isin(values))]
            else:
                return index[(index["key"] == key) & (index['value'].isin(values))]
        else:  # assuming value = "*"
            if isinstance(self.key, list):
                return index[index["key"].isin(key)]
            else:
                return index[index["key"] == key]

    def get_data(self):

        if self.data is not None:
            return self.data

        funcs = {
            'concept': self._get_data_concept,
            'entity': self._get_data_entity,
            'datapoint': self._get_data_datapoint
        }

        self.data = funcs[self.dtype]()
        return self.data

    def get_data_copy(self):

        filtered = self.filter_key_value()
        ddf_path = self.ddf_path

        res = []

        for f in set(filtered['file'].values):
            df = pd.read_csv(os.path.join(ddf_path, f), dtype=str)
            res.append([f, df])

        return dict(res)

    def reset_data(self):
        self.data = None

    def _get_data_datapoint(self):
        ddf_path = self.ddf_path

        filtered = self.filter_key_value()

        res = []

        for i, row in filtered.iterrows():
            df = pd.read_csv(os.path.join(ddf_path, row['file']))
            # TODO:
            # 1. know more about the row_filter syntax
            # 2. The query() Method is Experimental
            if self.row_filter:
                query_str = "and".join(["{} in {}".format(k, v) for k, v in self.row_filter.items()])
                # print(query_str)
                df = df.query(query_str)

            res.append([row['value'], df])

        return dict(res)

    def _get_data_concept(self):
        ddf_path = self.ddf_path

        filtered = self.filter_key_value()

        for f in set(filtered['file'].values):
            if 'continuous' in f:
                key = 'continuous'
            elif 'discrete' in f:
                key = 'discrete'
            else:
                key = 'concept'

            res = []
            df = pd.read_csv(os.path.join(ddf_path, f))

            if isinstance(self.values, list):
                df = df[self.values]

            if self.row_filter:
                query_str = "and".join(["{} in {}".format(k, v) for k, v in self.row_filter.items()])
                # print(query_str)
                df = df.query(query_str)

            res.append([key, df])
            return dict(res)

    def _get_data_entity(self):
        ddf_path = self.ddf_path

        filtered = self.filter_key_value()

        res = []

        for f in set(filtered['file'].values):
            entity = f[:-4].split('--')[-1]

            df = pd.read_csv(os.path.join(ddf_path, f))

            if isinstance(self.values, list):
                df = df[self.values]

            if self.row_filter:
                query_str = "and".join(["{} in {}".format(k, v) for k, v in self.row_filter.items()])
                # print(query_str)
                df = df.query(query_str)

            res.append([entity, df])

        return dict(res)


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
        from index import create_index_file
        print("no index file, creating one...")
        return create_index_file(ddf_path)


def _translate_header(ingredient, result, **options):

    global DICT_PATH

    dictionary = options['dictionary']

    di = ingredient.get_data().copy()

    if isinstance(dictionary, dict):
        rm = dictionary
    else:
        rm = json.load(open(os.path.join(DICT_PATH, dictionary), 'r'))

    for k, df in di.items():

        if k in rm.keys():
            di[rm[k]] = di[k].rename(columns=rm)
            del(di[k])

        else:
            di[k] = di[k].rename(columns=rm)

    return Ingredient(result, result, ingredient.key, "*", data=di)


def _translate_column(ingredient, result, **options):

    global DICT_PATH

    dictionary = options['dictionary']
    column = options['column']

    di = ingredient.get_data().copy()

    if isinstance(dictionary, dict):
        rm = dictionary
    else:
        rm = json.load(open(os.path.join(DICT_PATH, dictionary), 'r'))

    for k, df in di.items():

        df = df.set_index(column)
        di[k] = df.rename(index=rm).reset_index()

    return Ingredient(result, result, ingredient.key, "*", data=di)


def _merge(left, right, **options):

    deep = options['deep']

    left_data = left.get_data().copy()
    right_data = right.get_data().copy()

    assert left.dtype == right.dtype

    if left.dtype == 'datapoint':

        if deep:
            for k, df in right_data.items():
                if k in left_data.keys():
                    left_data[k].update(df)
                else:
                    left_data[k] = df
        else:
            for k, df in right_data.items():
                left_data[k] = df

        return left_data

    elif left.dtype == 'concept':

        left_df = pd.concat(left_data.values())
        right_df = pd.concat(right_data.values())

        if deep:
            left_df = left_df.merge(right_df, how='outer')
            return left_df
        else:
            return right_df

    else:
        # TODO
        raise ValueError('entity data do not support merging yet.')


def _identity(ingredient):
    return ingredient.get_data_copy()


## functions for reading/running recipe
def run_recipe(recipe_file):
    if re.match('.*\.json', recipe_file):
        recipe = json.load(open(recipe_file))
    else:
        recipe = yaml.load(open(recipe_file))

    # load ingredients
    ings = [Ingredient.from_dict(i) for i in recipe['ingredients']]
    ings_dict = dict([[i.ingred_id, i] for i in ings])

    # cooking
    funcs = {
        'translate_column': _translate_column,
        'translate_header': _translate_header,
        'identity': _identity,
        'merge': _merge
    }

    res = {}

    for k, pceds in recipe['cooking'].items():

        print("running "+k)

        for p in pceds:
            func = p['procedure']
            ingredient = [ings_dict[i] for i in p['ingredients']]

            if 'result' in p.keys():
                result = p['result']
                if 'options' in p.keys():
                    options = p['options']
                    ings_dict[result] = funcs[func](*ingredient, result, **options)
                else:
                    ings_dict[result] = funcs[func](*ingredient, result)
            else:
                if 'options' in p.keys():
                    options = p['options']
                    out = funcs[func](*ingredient, **options)
                else:
                    out = funcs[func](*ingredient)

        res[k] = out

    return res


def dish_to_csv(dishes, outpath):
    for t, dish in dishes.items():

        # get the key for datapoint
        if t == 'datapoints':
            name_tmp = list(dish.keys())[0]
            df_tmp = dish[name_tmp]
            by = df_tmp.columns.drop(name_tmp)
        else:
            by = None

        if isinstance(dish, dict):
            for k, df in dish.items():
                if re.match('ddf--.*.csv', k):
                    path = os.path.join(outpath, k)
                else:
                    if by is not None:
                        path = os.path.join(outpath, 'ddf--{}--{}--by--{}.csv'.format(t, k, '--'.join(by)))
                    else:
                        path = os.path.join(outpath, 'ddf--{}--{}.csv'.format(t, k))

                if t == 'datapoints':
                    df.to_csv(path, index=False, float_format='%.2f')
                else:
                    df.to_csv(path, index=False)
        else:
            path = os.path.join(outpath, 'ddf--{}.csv'.format(t))
            dish.to_csv(path, index=False)
