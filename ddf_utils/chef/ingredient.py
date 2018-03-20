# -*- coding: utf-8 -*-

"""main ingredient class"""

import fnmatch
import logging
import os
from collections import Mapping, Sequence

import numpy as np
import pandas as pd

import dask.dataframe as dd
from ddf_utils.model.package import Datapackage
from ddf_utils.model.repo import Repo, is_url

from ..str import format_float_digits
from .exceptions import IngredientError
from .helpers import gen_sym, query, read_opt, sort_df


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

    def compute(self):
        res = dict()
        if self.data is None:
            self.get_data()
        for k, v in self.data.items():
            if isinstance(v, dd.DataFrame):
                res[k] = v.compute()
            else:
                res[k] = v
        return res

    def get_data(self):
        return self.data

    def reset_data(self):
        self.data = None
        return

    def _serve_concepts(self, outpath, **options):
        data = self.compute()
        assert isinstance(data, dict)
        assert len(data) == 1
        for _, df in data.items():
            # change boolean into string
            # and remove tailing spaces
            for i, v in df.dtypes.iteritems():
                if v == 'bool':
                    df[i] = df[i].map(lambda x: str(x).upper())
                if v == 'object':
                    df[i] = df[i].str.strip()
            path = os.path.join(outpath, 'ddf--concepts.csv')
            df = sort_df(df, key='concept')
            df.to_csv(path, index=False, encoding='utf8')

    def _serve_entities(self, outpath, **options):
        data = self.compute()
        assert isinstance(data, dict)
        assert len(data) == 1
        sets = []
        no_keep_sets = options.get('no_keep_sets', False)  # serve as entity domain
        for k, df in data.items():
            # change boolean into string
            for c in df.columns:
                if df.dtypes[c] == 'bool':  # inferred boolean values
                    df[c] = df[c].map(lambda x: str(x).upper() if not pd.isnull(x) else x)
                if c.startswith('is--'):  # is-- columns
                    if no_keep_sets:
                        df = df.drop(c, axis=1)
                    else:
                        sets.append(c[4:])
                        df[c] = df[c].map(lambda x: str(x).upper() if not pd.isnull(x) else x)
            domain = self.key
            if k == domain:
                if len(sets) == 0:
                    path = os.path.join(outpath, 'ddf--entities--{}.csv'.format(k))
                    df = sort_df(df, key=domain)
                    df.to_csv(path, index=False, encoding='utf8')
                else:
                    for s in sets:
                        path = os.path.join(outpath, 'ddf--entities--{}--{}.csv'.format(k, s))
                        col = 'is--'+s
                        df_ = df[df[col]=='TRUE'].dropna(axis=1, how='all')
                        df_ = df_.loc[:, lambda x: ~x.columns.str.startswith('is--')].copy()
                        df_[col] = 'TRUE'
                        df_ = df_.rename({k: s}, axis=1)  # use set name as primary key column name
                        df_ = sort_df(df_, key=s)
                        df_.to_csv(path, index=False, encoding='utf8')
                    # serve entities not in any sets
                    is_headers = list(map(lambda x: 'is--'+x, sets))
                    noset = []
                    for i, row in df.iterrows():
                        # import pdb; pdb.set_trace()
                        if (row[is_headers].fillna('FALSE') == 'FALSE').all():
                            noset.append(i)
                    if len(noset) > 0:
                        df_noset = df.loc[noset].drop(is_headers, axis=1).dropna(axis=1, how='all')
                        path = os.path.join(outpath, 'ddf--entities--{}.csv'.format(k))
                        df_noset = sort_df(df_noset, key=domain)
                        df_noset.to_csv(path, index=False)
            else:
                # FIXME: is it even possible that self.key(domain) is not same as k?
                path = os.path.join(outpath, 'ddf--entities--{}--{}.csv'.format(domain, k))
                df = sort_df(df, key=k)
                df.to_csv(path, index=False, encoding='utf8')

    def _serve_datapoints(self, outpath, **options):
        data = self.compute()
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
            df = sort_df(df, key=by)
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
    see the implement of `Ingredient.from_dict()` method for how the object is constructed.

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

    ``value`` and ``filter`` can accept mongo like queries to make more complex statements, for example:

    .. code-block:: yaml

       id: example-ingredient
       dataset: ddf--example--dataset
       key: geo, time
       value:
           $nin:  # exclude following indicators
               - concept1
               - concept2
       filter:
           geo:
               $in:
                   - swe
                   - usa
                   - chn
           year:
               $and:
                   $gt: 2000
                   $lt: 2015

    for now, value accepts ``$in`` and ``$nin`` keywords, but only one of them can be in the value option;
    filter supports logical keywords: ``$and``, ``$or``, ``$not``, ``$nor``, and comparision keywords:
    ``$eq``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$ne``, ``$in``, ``$nin``.

    The other way to define the ingredient data is using the ``data`` keyword to include external csv file, or
    inline the data in the ingredient definition. Example:

    .. code-block:: yaml

       id: example-ingredient
       key: concept
       data: external_concepts.csv

    On-the-fly ingredient:

    .. code-block:: yaml

       id: example-ingredient
       key: concept
       data:
           - concept: concept_1
             name: concept_name_1
             concept_type: string
             description: concept_description_1
           - concept: concept_2
             name: concept_name_2
             concept_type: measure
             description: concept_description_2

    Attributes
    ----------
    ingred_id : `str`
        The id string
    ddf_id : `str`, optional
        the underlying dataset id
    key : `str`
        the key columns of the ingredient
    values : `str` or `list` or `dict`, optional
        concept filter applied to the dataset. if `value` == "*", all concept of
        the dataset will be in the ingredient
    row_filter : `dict`, optional
        row filter applied to the dataset
    data : `dict`, optional
        the data in the ingredient

    Methods
    -------
    get_data()
        read in and return the ingredient data

    """
    def __init__(self, chef=None, ingred_id=None, ddf_id=None, data_def=None,
                 key=None, values=None, row_filter=None, data=None, dry_run=False):
        super(Ingredient, self).__init__(chef, ingred_id, key, data)
        self.values = values
        self.row_filter = row_filter
        self.data_def = data_def
        self._ddf_id = ddf_id
        self._ddf = None
        self.is_dry_run = dry_run

    @classmethod
    def from_dict(cls, chef, dictionary):
        """create an instance by a dictionary

        The dictionary should provide following keys:

        - id
        - dataset or data
        - key
        - value (optional)
        - filter (optional)

        if dataset is provided, data will be read from ddf dataset; if data is provided, then either data will be read
        from a local csv file or be created on-the-fly base on the description.
        """
        ingred_id = read_opt(dictionary, 'id', default=None)
        dataset = read_opt(dictionary, 'dataset', default=None)
        data_def = read_opt(dictionary, 'data', default=None)
        key = read_opt(dictionary, 'key', required=True)
        values = read_opt(dictionary, 'value', default='*')
        row_filter = read_opt(dictionary, 'filter', default=None)

        if ingred_id is None:
            ingred_id = gen_sym('inline', None, dictionary)

        if dataset is not None:
            assert data_def is None, 'one of `dataset` and `data` should be provided'
        else:
            assert data_def is not None, 'one of `dataset` and `data` should be provided'

        if len(dictionary.keys()) > 0:
            logging.warning("Ignoring following keys: {}".format(list(dictionary.keys())))

        if dataset is not None:  # data will read from ddf dataset
            if is_url(dataset):
                repo = Repo(dataset, base_path=chef.config.get('ddf_dir', './'))
                ddf_id = repo.name
            else:
                ddf_id = dataset

            return cls(chef, ingred_id, ddf_id=ddf_id, key=key, values=values, row_filter=row_filter)
        else:  # data will be read from csv file or create on-the-fly
            return cls(chef, ingred_id, data_def=data_def, key=key, values=values, row_filter=row_filter)

    @property
    def ddf(self):
        """The DDF reader object"""
        if self._ddf:
            return self._ddf
        else:
            if self._ddf_id:
                if self._ddf_id not in self.chef.ddf_object_cache.keys():
                    self._ddf = Datapackage(
                        os.path.join(self.chef.config['ddf_dir'], self._ddf_id)).load(no_datapoints=self.is_dry_run)
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
            if isinstance(self.values, Sequence):  # just a list of indicators to include
                for i in self.values:
                    if i in self.ddf.indicators(by=keys):
                        data[i] = self.ddf.get_datapoint_df(i, primary_key=keys)
                    else:
                        logging.warning("indicator {} not found in dataset {}".format(i, self._ddf_id))
            else:  # a dictionary with queries
                assert len(self.values) == 1
                assert list(self.values.keys())[0] in ['$in', '$nin']
                for keyword, items in self.values.items():
                    if keyword == '$in':
                        for i in items:
                            matches = fnmatch.filter(self.ddf.indicators(by=keys), i)
                            if len(matches) == 0:
                                logging.warning("indicator matching {} not found in dataset {}".format(i, self._ddf_id))
                                continue
                            for m in matches:
                                data[m] = self.ddf.get_datapoint_df(m, primary_key=keys)
                    else:
                        all_indicators = self.ddf.indicators(by=keys)
                        matches = set(all_indicators) - set(fnmatch.filter(all_indicators, items[0]))
                        for i in items[1:]:
                            matches = matches - set(fnmatch.filter(list(matches), i))
                        if len(matches) == 0:
                            logging.warning("indicators matching the value descriptor not "
                                            "found in dataset " + self._ddf_id)
                            continue
                        for m in matches:
                            data[m] = self.ddf.get_datapoint_df(m, primary_key=keys)
        if len(data) == 0:
            raise IngredientError('no datapoint found for the ingredient: ' + self.ingred_id)

        return data

    # because concept ingerdient and entity ingerdient only have one key in the
    # data dictionary, so they don't need to support the column filter
    def _get_data_entities(self):
        df = self.ddf.get_entity(self.key)
        if self.values != '*':
            if isinstance(self.values, Mapping):
                assert len(self.values) == 1
                assert list(self.values.keys())[0] in ['$in', '$nin']
                kw = list(self.values.keys())[0]
                if kw == ['$in']:
                    return {self.key: df[self.values[kw]]}
                else:
                    return {self.key: df[df.columns.drop(self.values[kw])]}
            else:
                return {self.key: df[self.values]}
        else:
            return {self.key: df}

    def _get_data_concepts(self):
        df = self.ddf.concepts
        if self.values != '*':
            if isinstance(self.values, Mapping):
                assert len(self.values) == 1
                assert list(self.values.keys())[0] in ['$in', '$nin']
                kw = list(self.values.keys())[0]
                if kw == ['$in']:
                    return {'concept': df[self.values[kw]]}
                else:
                    return {'concept': df[df.columns.drop(self.values[kw])]}
            else:
                return {'concept': df[self.values]}
        else:
            return {'concept': df}

    def get_data(self, copy=False, key_as_index=False):
        """read in and return the ingredient data
        """
        if self._ddf_id:
            self.data = self._get_data_ddf(copy, key_as_index)
        else:
            self.data = self._get_data_external(copy, key_as_index)
        return self.data

    def _get_data_external(self, copy=False, key_as_index=False):
        """read data from csv or on-the-fly"""
        if isinstance(self.data_def, str):  # it should be a file name
            df = pd.read_csv(self.data_def)
        else:  # it should be a dict
            df = pd.DataFrame.from_dict(self.data_def)

        data = {}
        if self.dtype == 'datapoints':
            df = df.set_index(self.key_to_list())
            for c in df.columns:
                data[c] = df[c].reset_index()
        else:
            data[self.key] = df

        # applying row filter
        if self.row_filter is not None:
            for k, df in data.items():
                data[k] = query(df, self.row_filter, available_scopes=df.columns)

        self.data = data
        return self.data

    def _get_data_ddf(self, copy=False, key_as_index=False):
        """read data from ddf dataset"""
        funcs = {
            'datapoints': self._get_data_datapoint,
            'entities': self._get_data_entities,
            'concepts': self._get_data_concepts
        }

        def filter_row(df, avaliable_scopes):
            """return the rows selected by self.row_filter."""
            if self.row_filter:
                df = query(df, self.row_filter, avaliable_scopes)
            return df

        if self.data is None:
            data = funcs[self.dtype]()
            if self.dtype == 'datapoints':
                for k, v in data.items():
                    data[k] = v

            for k, v in data.items():
                if self.row_filter:
                    data[k] = filter_row(data[k], avaliable_scopes=data[k].columns)

            self.data = data
        if key_as_index:
            # TODO set index when requiring data
            pass
        return self.data


class ProcedureResult(BaseIngredient):
    def __init__(self, chef, ingred_id, key, data):
        assert isinstance(data, Mapping), "data should be dictionary, {} provided".format(type(data))
        super(ProcedureResult, self).__init__(chef, ingred_id, key, data)

    def __repr__(self):
        return '<ProcedureResult: {}>'.format(self.ingred_id)
