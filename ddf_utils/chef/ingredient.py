# -*- coding: utf-8 -*-

"""main ingredient class"""

import numpy as np
import pandas as pd
from ..str import format_float_digits
from .helpers import read_opt
import os
import logging

from ddf_utils.model.package import Datapackage
from .exceptions import IngredientError


class BaseIngredient(object):
    def __init__(self, chef, ingred_id, key, data=None):
        self.chef = chef
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

    def _serve_concepts(self, outpath, **options):
        data = self.copy_data()
        assert isinstance(data, dict)
        for k, df in data.items():
            # change boolean into string
            # and remove tailing spaces
            for i, v in df.dtypes.iteritems():
                if v == 'bool':
                    df[i] = df[i].map(lambda x: str(x).upper())
                if v == 'object':
                    df[i] = df[i].str.strip()
            path = os.path.join(outpath, 'ddf--concepts.csv')
            df.to_csv(path, index=False, encoding='utf8')

    def _serve_entities(self, outpath, **options):
        data = self.copy_data()
        assert isinstance(data, dict)
        sets = []
        no_keep_sets = options.get('no_keep_sets', False)
        for k, df in data.items():
            # change boolean into string
            # TODO: not only for is-- headers
            for c in df.columns:
                if df.dtypes[c] == 'bool':
                    df[c] = df[c].map(lambda x: str(x).upper() if not pd.isnull(x) else x)
                if c.startswith('is--'):
                    if no_keep_sets:
                        df = df.drop(c, axis=1)
                    else:
                        sets.append(c[4:])
                        df[c] = df[c].map(lambda x: str(x).upper() if not pd.isnull(x) else x)
            domain = self.key
            if k == domain:
                if len(sets) == 0:
                    path = os.path.join(outpath, 'ddf--entities--{}.csv'.format(k))
                    df.to_csv(path, index=False, encoding='utf8')
                else:
                    for s in sets:
                        path = os.path.join(outpath, 'ddf--entities--{}--{}.csv'.format(k, s))
                        col = 'is--'+s
                        df[df[col]=='TRUE'].dropna(axis=1, how='all').to_csv(path, index=False, encoding='utf8')
            else:
                path = os.path.join(outpath, 'ddf--entities--{}--{}.csv'.format(domain, k))
                df.to_csv(path, index=False, encoding='utf8')

    def _serve_datapoints(self, outpath, **options):
        data = self.copy_data()
        assert isinstance(data, dict)
        digits = read_opt(options, 'digits', default=5)

        def to_disk(df_input, k, path):
            df = df_input.copy()
            if not np.issubdtype(df[k].dtype, np.number):
                try:
                    df[k] = df[k].astype(float)
                    df[k] = df[k].map(lambda x: format_float_digits(x, digits))
                except ValueError:
                    logging.warning("data not numeric: " + k)
            else:
                df[k] = df[k].map(lambda x: format_float_digits(x, digits))
            df.to_csv(path, encoding='utf8', index=False)

        for k, df in data.items():
            split_by = read_opt(options, 'split_datapoints_by', default=False)
            by = self.key_to_list()
            if not split_by:
                columns = [*by, k]
                path = os.path.join(
                    outpath,
                    'ddf--datapoints--{}--by--{}.csv'.format(k, '--'.join(by)))
                to_disk(df[columns], k, path)
            else:
                # split datapoints by entities. Firstly we calculate all possible
                # combinations of entities, and then filter the dataframe, create
                # file names and save them into disk.
                from itertools import product
                values = list()
                [values.append(df[col].unique()) for col in split_by]
                all_combinations = product(*values)

                if len(by) > len(split_by):
                    by = list(set(by) - set(split_by))
                    columns = [*sorted(split_by), *sorted(by), k]
                else:
                    by = None
                    columns = [*sorted(split_by), k]

                for comb in all_combinations:
                    query = ''
                    entity_strings = list()
                    for entity, value in zip(split_by, comb):
                        entity_strings.append(entity + '-' + value)
                        if len(query) > 0:
                            query = query + " and " + "{} == '{}'".format(entity, value)
                        else:
                            query = "{} == '{}'".format(entity, value)

                    if by:
                        path = os.path.join(
                            outpath,
                            'ddf--datapoints--{}--by--{}--{}.csv'.format(
                                k,
                                '--'.join(sorted(entity_strings)),
                                '--'.join(sorted(by))
                            )
                        )
                    else:
                        path = os.path.join(
                            outpath,
                            'ddf--datapoints--{}--by--{}.csv'.format(
                                k,
                                '--'.join(sorted(entity_strings))
                            )
                        )
                    # logging.debug('query is: ' + query)
                    df_part = df.query(query)
                    to_disk(df_part[columns], k, path)

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
        path : `str`
            which sub-folder under the outpath to save the output files

        """
        logging.info('serving ingredient: ' + self.ingred_id)
        # create outpath if not exists
        if 'path' in options:
            sub_folder = options.pop('path')
            assert not os.path.isabs(sub_folder)  # sub folder should not be abspath
            outpath = os.path.join(outpath, sub_folder)
        os.makedirs(outpath, exist_ok=True)

        t = self.dtype
        if t == 'datapoints':
            self._serve_datapoints(outpath, **options)
        elif t == 'concepts':
            self._serve_concepts(outpath, **options)
        elif t == 'entities':
            self._serve_entities(outpath, **options)
        else:
            raise IngredientError('Not a correct collection: ' + t)


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
    filter : `dict`
        row filter applied to the dataset

    Methods
    -------
    get_data()
        read in and return the ingredient data

    """
    def __init__(self, chef, ingred_id,
                 ddf_id=None, key=None, values=None, row_filter=None, data=None):
        super(Ingredient, self).__init__(chef, ingred_id, key, data)
        self.values = values
        self.row_filter = row_filter
        self._ddf_id = ddf_id
        self._ddf = None

    @classmethod
    def from_dict(cls, chef, data):
        """create an instance by a dictionary

        The dictionary should provide following keys:

        - id
        - dataset
        - key
        - value
        - filter (optional)
        """
        ingred_id = read_opt(data, 'id', required=True)
        ddf_id = read_opt(data, 'dataset', required=True)
        key = read_opt(data, 'key', required=True)
        values = read_opt(data, 'value', required=True)
        row_filter = read_opt(data, 'filter', required=False, default=None)

        if len(data.keys()) > 0:
            logging.warning("Ignoring following keys: {}".format(list(data.keys())))

        return cls(chef, ingred_id, ddf_id, key, values, row_filter)

    @property
    def ddf(self):
        """The DDF reader object"""
        if self._ddf:
            return self._ddf
        else:
            if self._ddf_id:
                if self._ddf_id not in self.chef.ddf_object_cache.keys():
                    self._ddf = Datapackage(os.path.join(self.chef.config['ddf_dir'], self._ddf_id)).load()
                    self.chef.ddf_object_cache[self._ddf_id] = self._ddf
                else:
                    self._ddf = self.chef.ddf_object_cache[self._ddf_id]
                # self._ddf = DDF(os.path.join(self.chef.config['ddf_dir'], self._ddf_id))
                return self._ddf
        return None

    @property
    def ddf_id(self):
        return self._ddf_id

    @property
    def dataset_path(self):
        """The path to the dataset"""
        return self._ddf.base_dir

    def __repr__(self):
        return '<Ingredient: {}>'.format(self.ingred_id)

    def _get_data_datapoint(self):
        data = dict()
        keys = self.key_to_list()

        if self.values == '*':
            for i in self.ddf.indicators(by=keys):
                data[i] = self.ddf.get_datapoint_df(i, primary_key=keys)
        else:
            for i in self.values:
                if i in self.ddf.indicators(by=keys):
                    data[i] = self.ddf.get_datapoint_df(i, primary_key=keys)
                else:
                    logging.warning("indicator {} not found in dataset {}".format(i, self._ddf_id))

        if len(data) == 0:
            raise IngredientError('no datapoint found for the ingredient: ' + self.ingred_id)

        return data

    def _get_data_entities(self):
        return {self.key: self.ddf.get_entity(self.key)}

    def _get_data_concepts(self):
        if self.values == '*':
            return {'concepts': self.ddf.concepts}
        else:
            return {'concepts': self.ddf.concepts[self.values]}

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
                # logging.debug(query_str)
                df = df.query(query_str)
                # logging.debug(df.head())
            return df

        if self.data is None:
            data = funcs[self.dtype]()
            if self.dtype == 'datapoints':
                for k, v in data.items():
                    data[k] = v.compute()

            for k, v in data.items():
                if self.row_filter:
                    # index_cols = data[k].index.names
                    # data[k] = self.filter_row(data[k].reset_index()).set_index(index_cols)
                    data[k] = filter_row(data[k])

            self.data = data
        if key_as_index:
            # TODO set index when requiring data
            pass
        return self.data


class ProcedureResult(BaseIngredient):
    def __init__(self, chef, ingred_id, key, data):
        super(ProcedureResult, self).__init__(chef, ingred_id, key, data)

    def __repr__(self):
        return '<ProcedureResult: {}>'.format(self.ingred_id)

    def reset_data(self):
        # TODO: allowing reset data? It can not be reconstructed.
        raise NotImplementedError('')
