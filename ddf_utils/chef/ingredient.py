# -*- coding: utf-8 -*-

"""main ingredient class"""

import pandas as pd
import numpy as np
from ..str import format_float_digits
from .helpers import read_opt
import os
import logging

from .. import config
from ..ddf_reader import DDF
from .exceptions import *


class BaseIngredient(object):
    def __init__(self, ingred_id, key, data=None):
        self.ingred_id = ingred_id
        self.key = key
        self.data = data

    @property
    def dtype(self):
        """
        returns the type of ddf data, i.e. concepts/entities/datapoints.

        It will be inferred from the key property of ingredient.
        """
        # TODO: what if key == '*'? Is it possible?
        keys = self.key_to_list()
        if len(keys) == 1:
            if keys[0] == 'concept':
                return 'concepts'
            else:
                return 'entities'
        else:
            return 'datapoints'

    def key_to_list(self):
        """helper function: make a list that contains primaryKey of this ingredient"""
        return [x.strip() for x in self.key.split(',')]

    def get_data(self):
        return self.data

    def copy_data(self):
        """this function makes copy of self.data.
        """
        if not self.data:
            self.get_data()
        # v: DataFrame. DataFrame.copy() is by default deep copy,
        # but I just call it explicitly here.
        return dict((k, v.copy(deep=True)) for k, v in self.data.items())

    def reset_data(self):
        self.data = None
        return

    def serve(self, outpath, **options):
        """save the ingledient to disk.

        Parameters
        ----------
        outpath : str
            the path to save the ingredient

        Other Parameters
        ----------------
        digits : int
            how many digits to keep at most.

        """
        # create outpath if not exists
        os.makedirs(outpath, exist_ok=True)

        data = self.copy_data()
        t = self.dtype
        assert isinstance(data, dict)
        for k, df in data.items():
            # change boolean into string
            for i, v in df.dtypes.iteritems():
                if v == 'bool':
                    df[i] = df[i].map(lambda x: str(x).upper())
            if t == 'datapoints':
                by = self.key_to_list()
                path = os.path.join(outpath, 'ddf--{}--{}--by--{}.csv'.format(t, k, '--'.join(by)))
            elif t == 'concepts':
                path = os.path.join(outpath, 'ddf--{}.csv'.format(t))
            elif t == 'entities':
                domain = self.key
                if k == domain:
                    path = os.path.join(outpath, 'ddf--{}--{}.csv'.format(t, k))
                else:
                    path = os.path.join(outpath, 'ddf--{}--{}--{}.csv'.format(t, domain, k))
            else:
                raise IngredientError('Not a correct collection: ' + t)
            # formatting numbers for datapoints
            if t == 'datapoints':
                digits = read_opt(options, 'digits', default=5)
                df = df.set_index(by)
                if not np.issubdtype(df[k].dtype, np.number):
                    try:
                        df[k] = df[k].astype(float)
                        df[k] = df[k].map(lambda x: format_float_digits(x, digits))
                    except ValueError:
                        logging.warning("data not numeric: " + k)
                else:
                    df[k] = df[k].map(lambda x: format_float_digits(x, digits))
                df[[k]].to_csv(path, encoding='utf8')
            else:
                df.to_csv(path, index=False, encoding='utf8')


class Ingredient(BaseIngredient):
    """
    ingredient class: represents an ingredient object in recipe file.
    see the implement of from_dict() method for how the object is constructed.

    Here is an example ingredient object in recipe:

    .. code-block:: yaml

        id: example-ingredient
        dataset: ddf--example--dataset
        key: "geo,time"  # key columns of ingredient
        value:  # only include concepts listed here
          - concept_1
          - concept_2
        filter:  # select rows by column values
          geo:  # only keep datapoint where `geo` is in [swe, usa, chn]
            - swe
            - usa
            - chn

    Attributes
    ----------
    ingred_id : `str`
        The id string
    ddf_id : `str`
        the underlaying dataset id
    key : `str`
        the key columns of the ingredient
    value : `list`
        concept filter applied to the dataset. if `value` == "*", all concept of
        the dataset will be in the ingredient
    row_filter : `dict`
        row filter applied to the dataset

    Methods
    -------
    get_data()
        read in and return the ingredient data

    """
    def __init__(self, ingred_id,
                 ddf_id=None, key=None, values=None, row_filter=None, data=None):
        super(Ingredient, self).__init__(ingred_id, key, data)
        self.values = values
        self.row_filter = row_filter
        self._ddf_id = ddf_id
        self._ddf = None

    @classmethod
    def from_dict(cls, data):
        """create an instance by a dictionary

        The dictionary should provide following keys:

        - id
        - dataset
        - key
        - value
        - filter (optional)
        """
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
        """The DDF reader object"""
        if self._ddf:
            return self._ddf
        else:
            if self._ddf_id:
                self._ddf = DDF(self._ddf_id)
                return self._ddf
        return None

    @property
    def ddf_path(self):
        """The path to the dataset"""
        return self.ddf.dataset_path

    def __repr__(self):
        return '<Ingredient: {}>'.format(self.ingred_id)

    def _get_data_datapoint(self, copy):
        data = dict()
        keys = self.key_to_list()
        if self.values == '*':  # get all datapoints for the key
            values = []
            for k, v in self.ddf.get_datapoint_files().items():
                for pkeys in v.keys():
                    if set(keys) == set(pkeys):
                        values.append(k)
        else:
            values = self.values

        if not values or len(values) == 0:
            raise IngredientError('no datapoint found for the ingredient: ' + self.ingred_id)

        for v in values:
            data[v] = self.ddf.get_datapoint_df(v, keys)
        return data

    def _get_data_entities(self, copy):
        # TODO: values should always be '*'?
        if copy:
            ent = self.ddf.get_entities(dtype=str)
        else:
            ent = self.ddf.get_entities()
        conc = self.ddf.get_concepts()
        values = []

        if conc.ix[self.key, 'concept_type'] == 'entity_domain':
            if 'domain' in conc.columns and len(conc[conc['domain'] == self.key]) > 0:
                [values.append(i) for i in conc[conc['domain'] == self.key].index]
            else:
                values.append(self.key)
        else:
            values.append(self.key)

        return dict((k, ent[k]) for k in values)

    def _get_data_concepts(self, copy):
        if self.values == '*':
            return {'concepts': self.ddf.get_concepts()}
        else:
            return {'concepts': self.ddf.get_concepts()[self.values]}


    def get_data(self, copy=False, key_as_index=False):
        """read in and return the ingredient data
        """
        funcs = {
            'datapoints': self._get_data_datapoint,
            'entities': self._get_data_entities,
            'concepts': self._get_data_concepts
        }

        def filter_row(df):
            """return the rows selected by self.row_filter."""
            # TODO: improve filtering function
            # 1. know more about the row_filter syntax
            # 2. The query() Method is Experimental
            if self.row_filter:
                query_str = "and".join(["{} in {}".format(k, v) for k, v in self.row_filter.items()])
                # print(query_str)
                df = df.query(query_str)
            return df

        if self.data is None:
            data = funcs[self.dtype](copy)
            for k, v in data.items():
                if self.row_filter:
                    index_cols = data[k].index.names
                    # data[k] = self.filter_row(data[k].reset_index()).set_index(index_cols)
                    data[k] = filter_row(data[k].reset_index())
                else:
                    data[k] = data[k].reset_index()
            self.data = data
        if key_as_index:
            # TODO set index when requiring data
            pass
        return self.data


class ProcedureResult(BaseIngredient):
    def __init__(self, ingred_id, key, data):
        super(ProcedureResult, self).__init__(ingred_id, key, data)

    def __repr__(self):
        return '<ProcedureResult: {}>'.format(self.ingred_id)

    def reset_data(self):
        # TODO: allowing reset data? It can not be reconstructed.
        raise NotImplementedError('')
