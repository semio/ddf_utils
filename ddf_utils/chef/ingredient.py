# -*- coding: utf-8 -*-

"""main ingredient class"""

import numpy as np
from ..str import format_float_digits
from .helpers import read_opt
import os
import logging

from ..ddf_reader import DDF
from .exceptions import IngredientError


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

    def _serve_concepts(self, outpath, **options):
        data = self.copy_data()
        assert isinstance(data, dict)
        for k, df in data.items():
            # change boolean into string
            for i, v in df.dtypes.iteritems():
                if v == 'bool':
                    df[i] = df[i].map(lambda x: str(x).upper())
            path = os.path.join(outpath, 'ddf--concepts.csv')
            df.to_csv(path, index=False, encoding='utf8')

    def _serve_entities(self, outpath, **options):
        data = self.copy_data()
        assert isinstance(data, dict)
        for k, df in data.items():
            # change boolean into string
            for i, v in df.dtypes.iteritems():
                if v == 'bool':
                    df[i] = df[i].map(lambda x: str(x).upper())
            domain = self.key
            if k == domain:
                path = os.path.join(outpath, 'ddf--entities--{}.csv'.format(k))
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

        """
        logging.info('serving ingredient: ' + self.ingred_id)
        # create outpath if not exists
        if 'sub_folder' in options:
            sub_folder = options.pop('sub_folder')
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
        if 'row_filter' in data.keys():
            row_filter = data['row_filter']
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
                # logging.debug(query_str)
                df = df.query(query_str)
                # logging.debug(df.head())
            return df

        if self.data is None:
            data = funcs[self.dtype](copy)
            for k, v in data.items():
                if self.row_filter:
                    # index_cols = data[k].index.names
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
