# -*- coding: utf-8 -*-

"""main ingredient class"""

import pandas as pd
import os
import logging

from . import config


class Ingredient(object):
    """
    ingredient class: represents an ingredient object in recipe file.
    see the implement of from_dict() method for how the object is constructed.
    """
    def __init__(self, ingred_id, ddf_id, key, values, row_filter=None, data=None):
        self.ingred_id = ingred_id
        self.ddf_id = ddf_id
        self.key = key
        self.values = values
        self.row_filter = row_filter
        self.data = data
        # last_update is a dataframe contains last update
        # time for each file in this ingredient.
        self.last_update = None

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
    def index(self):
        return _get_index(self.ddf_id)

    @property
    def dtype(self):
        """
        returns the type of ddf data, i.e. concepts/entities/datapoints.

        It will be inferred from the key property of ingredient.
        TODO: what if key == '*'? Is it possible?
        """
        if self.key == 'concept':
            return 'concepts'
        elif isinstance(self.key, list):
            return 'entities'
        else:
            return 'datapoints'

    def __repr__(self):
        lines = list()
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
        if self.dtype == "datapoints":
            return self.key.split(',')
        else:
            assert isinstance(self.key, str)
            return [self.key]

    def filter_index_key_value(self):
        """filter index file by key and value"""
        index = self.index
        key = self.key

        if isinstance(self.values, list):
            values = self.values
            if isinstance(self.key, list):
                filtered = index[(index["key"].isin(key)) & (index['value'].isin(values))]
            else:
                filtered = index[(index["key"] == key) & (index['value'].isin(values))]
        else:  # assuming value = "*"
            if isinstance(self.key, list):
                filtered = index[index["key"].isin(key)]
            else:
                filtered = index[index["key"] == key]

        if len(filtered) == 0:
            raise ValueError(self.ingred_id +
                             ": No data found for the key/value pair! check your ingredient.")

        return filtered

    def filter_index_file_name(self):
        """
        filter index by keywords in the file names.
        This method should only called for entities related tasks.
        """
        index = self.index
        key = self.key

        if isinstance(key, list):
            bools = []
            for k in key:
                bools.append(index['file'].str.contains(k))

            ser_0 = bools[0]
            for ser in bools[1:]:
                ser_0 = ser_0 | ser

            full_list = index[ser_0]
        else:
            full_list = index[index['file'].str.contains(key)]

        return full_list[full_list['file'].str.contains(self.dtype)]

    def filter_row(self, df):
        """return the rows selected by self.row_filter."""
        # TODO: improve filtering function
        # 1. know more about the row_filter syntax
        # 2. The query() Method is Experimental
        if self.row_filter:
            query_str = "and".join(["{} in {}".format(k, v) for k, v in self.row_filter.items()])
            # print(query_str)
            df = df.query(query_str)

        return df

    def get_data(self):

        # if data is not empty, then it will just return the data
        # when get_data() is called.
        if self.data is not None:
            return self.data

        funcs = {
            # all functions will return a dictionary of dataframes.
            'concepts': self._get_data_concept,
            'entities': self._get_data_entity,
            'datapoints': self._get_data_datapoint
        }

        self.data = funcs[self.dtype]()
        return self.data

    def get_data_str(self):
        """
        this function will read the related data as strings for all columns,
        using the filename as key in the result
        """
        if self.dtype == 'entities':
            filtered = self.filter_index_file_name()
        else:
            filtered = self.filter_index_key_value()

        ddf_path = self.ddf_path

        res = []

        for f in set(filtered['file'].values):
            df = pd.read_csv(os.path.join(ddf_path, f), dtype=str)
            res.append([f, df])

        return dict(res)

    def reset_data(self):
        self.data = None

    def copy_data(self):
        """this function makes copy of self.data.
        """
        if not self.data:
            self.get_data()
        # v: DataFrame. DataFrame.copy() is by default deep copy,
        # but I just call it explicitly here.
        return dict((k, v.copy(deep=True)) for k, v in self.data.items())

    def get_last_update(self):
        if self.last_update is not None:
            return self.last_update

        index = self.index

        for f in index['file'].drop_duplicates().values:
            path = os.path.join(config.SEARCH_PATH, self.ddf_id, f)
            mtime = os.path.getmtime(path)

            index.loc[index['file'] == f, 'last_update'] = mtime

        self.last_update = index[['file', 'last_update']].drop_duplicates()
        return self.last_update

    def _get_data_datapoint(self):
        ddf_path = self.ddf_path

        filtered = self.filter_index_key_value()

        res = []

        # map all index columns to string type
        keys = self.key_to_list()
        dtype_conf = dict([(x, str) for x in keys])

        for i, row in filtered.iterrows():
            df = pd.read_csv(os.path.join(ddf_path, row['file']), dtype=dtype_conf)
            df = self.filter_row(df)

            res.append([row['value'], df])

        return dict(res)

    def _get_data_concept(self):
        ddf_path = self.ddf_path

        filtered = self.filter_index_key_value()

        res = []
        for f in set(filtered['file'].values):
            if 'continuous' in f:
                key = 'continuous'
            elif 'discrete' in f:
                key = 'discrete'
            else:
                key = 'concept'

            df = pd.read_csv(os.path.join(ddf_path, f))

            if isinstance(self.values, list):
                df = df[self.values]

            df = self.filter_row(df)
            res.append([key, df])
        return dict(res)

    def _get_data_entity(self):
        ddf_path = self.ddf_path

        filtered = self.filter_index_file_name()

        res = []

        for f in set(filtered['file'].values):
            entity = f[:-4].split('--')[-1]

            # if re.match('ddf--entities--.*--.*.csv', f):
                # domain = re.match('ddf--entities--(.*)--.*.csv', f).groups()[0]

            df = pd.read_csv(os.path.join(ddf_path, f), dtype=str)

            if isinstance(self.values, list):
                df = df[self.values]
            df = self.filter_row(df)
            res.append([entity, df])

        return dict(res)


# helper functions for Ingredient
def _get_ddf_path(ddf_id):
    path = os.path.join(config.SEARCH_PATH, ddf_id)
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
        from .. index import create_index_file
        print("no index file, creating one...")
        return create_index_file(ddf_path)
