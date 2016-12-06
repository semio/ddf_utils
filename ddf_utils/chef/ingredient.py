# -*- coding: utf-8 -*-

"""main ingredient class"""

import pandas as pd
import os
import logging

from .. import config
from ..ddf_reader import DDF


class Ingredient(object):
    """
    ingredient class: represents an ingredient object in recipe file.
    see the implement of from_dict() method for how the object is constructed.
    """
    def __init__(self, ingred_id,
                 ddf_id=None, key=None, values=None, row_filter=None, data=None):
        self.ingred_id = ingred_id
        self.key = key
        self.values = values
        self.row_filter = row_filter
        self._ddf_id = ddf_id
        self._ddf = None
        self.data = data

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
    def ddf(self):
        if self._ddf:
            return self._ddf
        else:
            if self._ddf_id:
                self._ddf = DDF(self._ddf_id)
                return self._ddf
        return None

    @property
    def ddf_path(self):
        return self.ddf.dataset_path

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
        return '<Ingredient: {}>'.format(self.ingred_id)

    def _get_data_datapoint(self, copy):
        data = dict()
        keys = self.key.split(',')
        if self.values == '*':  # get all datapoints for the key
            values = []
            for k, v in self.ddf.get_datapoint_files().items():
                for pkeys in v.keys():
                    if set(keys) == set(pkeys):
                        values.append(k)
        else:
            values = self.values

        if not values or len(values) == 0:
            raise ValueError('no datapoint found for the ingredient: ' + self.ingred_id)

        for v in values:
            data[v] = self.ddf.get_datapoint_df(v, keys)
        return data

    def _get_data_entities(self, copy):
        # TODO: values should always be '*'?
        if copy:
            ent = self.ddf.get_entities(dtype=str)
        else:
            ent = self.ddf.get_entities()
        if self.key == '*':
            return ent
        else:
            conc = self.ddf.get_concepts()

            values = []

            for v in self.key:
                if conc.ix[v, 'concept_type'] == 'entity_domain':
                    if 'domain' in conc.columns and len(conc[conc['domain'] == v]) > 0:
                        [values.append(i) for i in conc[conc['domain'] == v].index]
                    else:
                        values.append(v)
                else:
                    values.append(v)

            return dict((k, ent[k]) for k in values)

    def _get_data_concepts(self, copy):
        if self.values == '*':
            return {'concepts': self.ddf.get_concepts()}
        else:
            return {'concepts': self.ddf.get_concepts()[self.values]}

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

    def get_data(self, copy=False):
        funcs = {
            'datapoints': self._get_data_datapoint,
            'entities': self._get_data_entities,
            'concepts': self._get_data_concepts
        }
        if self.data is None:
            data = funcs[self.dtype](copy)
            for k, v in data.items():
                if self.row_filter:
                    index_cols = data[k].index.names
                    # data[k] = self.filter_row(data[k].reset_index()).set_index(index_cols)
                    data[k] = self.filter_row(data[k].reset_index())
                else:
                    data[k] = data[k].reset_index()
            self.data = data
        return self.data

    def reset_data(self):
        self.data = None
        return

    def copy_data(self):
        """this function makes copy of self.data.
        """
        if not self.data:
            self.get_data()
        # v: DataFrame. DataFrame.copy() is by default deep copy,
        # but I just call it explicitly here.
        return dict((k, v.copy(deep=True)) for k, v in self.data.items())

    def key_to_list(self):
        """helper function: make a list that contains primaryKey of this ingredient"""
        if self.dtype == "datapoints":
            return self.key.split(',')
        else:
            assert isinstance(self.key, str)
            return [self.key]
