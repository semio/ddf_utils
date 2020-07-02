# -*- coding: utf-8 -*-

"""main ingredient class"""

__all__ = ['Ingredient', 'ConceptIngredient', 'EntityIngredient', 'DataPointIngredient',
           'ingredient_from_dict', 'key_to_list', 'get_ingredient_class']


import fnmatch
import logging
import os
from collections.abc import Mapping, Sequence
from itertools import product
from functools import singledispatch
from typing import Optional, Union, Dict

import shutil

import numpy as np
import pandas as pd
import dask.dataframe as dd

import attr
from abc import ABC, abstractmethod

from ddf_utils.model.package import DDFcsv
from ddf_utils.model.ddf import DDF
from ddf_utils.model.repo import Repo, is_url
from ddf_utils.str import format_float_digits
from ..exceptions import IngredientError
from ..helpers import gen_sym, query, read_opt, sort_df, read_local_ddf, create_dsk


logger = logging.getLogger('Ingredient')


@attr.s
class Ingredient(ABC):
    """Protocol class for all ingredients.

    all ingredients should have following format:

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

    The other way to define the ingredient data is using the ``data``
    keyword to include external csv file, or inline the data in the
    ingredient definition. Example:

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
    """
    # TODO: make some of these attributes _Frozen_
    # currently attrs doesn't support frozen attrs, check issue 133 for attrs
    id: str = attr.ib()
    key: Union[list, str] = attr.ib()
    value: Union[list, dict, str] = attr.ib(default='*')
    dataset: str = attr.ib(default=None)
    data: dict = attr.ib(default=None)
    row_filter: dict = attr.ib(default=None)
    base_dir: str = attr.ib(default='./')

    is_procedure_result: bool = attr.ib(default=False, init=False)  # if data is created by a procedure
    data_computed: dict = attr.ib(default=None, init=False)  # cached result from get_data() or a procedure.

    dtype = 'abc'

    @property
    def ingredient_type(self):
        if self.is_procedure_result:
            return 'procedure_result'
        if self.dataset is not None:
            return 'ddf'
        else:
            if isinstance(self.data, str):
                return 'external'
            else:
                return 'inline'

    @property
    def dataset_path(self):
        """return the full path to ingredient's dataset if the ingredient is from local ddf dataset."""
        if self.ingredient_type == 'ddf':
            if os.path.isabs(self.dataset):
                return self.dataset
            return os.path.join(self.base_dir, self.dataset)
        return None

    @property
    def ddf_id(self):
        if self.ingredient_type == 'ddf':
            return self.ddf.props.get('name', self.dataset)
        return None

    @property
    def ddf(self):
        if self.ingredient_type == 'ddf':
            ddf = read_local_ddf(self.dataset_path)
            return ddf
        return None

    @classmethod
    def from_procedure_result(cls, id, key, data_computed: dict):
        res = cls(id, key)
        for _, df in data_computed.items():
            if isinstance(key, list):
                for k in key:
                    assert k in df.columns, "the key {} not in data!".format(k)
            else:
                assert key in df.columns, "the key {} not in data!".format(key)
        res.data_computed = data_computed
        res.is_procedure_result = True
        return res

    @abstractmethod
    def get_data(self):
        ...

    @abstractmethod
    def serve(self, *args, **kwargs):
        """serving data to disk"""
        ...

    @staticmethod
    def filter_row(data: dict, row_filter):
        """return the rows selected by row_filter."""
        if row_filter is None:
            return data
        for k, df in data.items():
            data[k] = query(df, row_filter, available_scopes=df.columns)
        return data


@attr.s
class ConceptIngredient(Ingredient):
    dtype = 'concepts'

    def __attrs_post_init__(self):
        if isinstance(self.key, list):
            if len(self.key) != 1 or self.key[0] != 'concept':
                raise IngredientError('concept ingredient key must be "concept"')
        self.key = 'concept'

    def get_data(self) -> Dict[str, pd.DataFrame]:
        logger.info('evaluating data for {}'.format(self.id))
        if self.data_computed is not None:
            return self.data_computed

        ingredient_type = self.ingredient_type
        if ingredient_type == 'ddf':
            self.data_computed = self.get_data_from_ddf_dataset(self.dataset_path, self.value, self.row_filter)
        if ingredient_type == 'external':
            self.data_computed = self.get_data_from_external_csv(self.data, self.key, self.row_filter)
        if ingredient_type == 'inline':
            self.data_computed = self.get_data_from_inline_data(self.data, self.key, self.row_filter)

        return self.data_computed

    @staticmethod
    def get_data_from_ddf_dataset(dataset_path, value, row_filter):
        ddf = read_local_ddf(dataset_path)
        concepts = ddf.concepts
        df = pd.DataFrame.from_records([x.to_dict() for x in concepts.values()])
        if value != '*':
            if isinstance(value, Mapping):
                assert len(value) == 1
                assert list(value.keys())[0] in ['$in', '$nin']
                kw = list(value.keys())[0]
                if kw == '$in':
                    data = {'concept': df[value[kw]]}
                else:
                    data = {'concept': df[df.columns.drop(value[kw])]}
            else:
                data = {'concept': df[value]}
        else:
            data = {'concept': df}

        data = ConceptIngredient.filter_row(data, row_filter)
        return data

    @staticmethod
    def get_data_from_external_csv(file_path, key, row_filter):
        df = pd.read_csv(file_path)
        data = {key: df}
        data = ConceptIngredient.filter_row(data, row_filter)
        return data

    @staticmethod
    def get_data_from_inline_data(data, key, row_filter):
        df = pd.DataFrame.from_records(data)
        data = {key: df}
        data = ConceptIngredient.filter_row(data, row_filter)
        return data

    def serve(self, outpath, **options):
        if not self.data_computed:
            self.get_data()
        filename = read_opt(options, 'file_name', default='ddf--concepts.csv')
        subpath = read_opt(options, 'path', default=None)
        custom_column_order = read_opt(options, 'custom_column_order', default=None)
        outpath = _handel_subpath(outpath, subpath)
        logger.info('serving: {}'.format(self.id))
        for _, df_ in self.data_computed.items():
            # change boolean into string
            # and remove tailing spaces
            df = df_.copy()
            for i, v in df.dtypes.iteritems():
                if v == 'bool':
                    df[i] = df[i].map(lambda x: str(x).upper())
                if v == 'object':
                    df[i] = df[i].str.strip()
            path = os.path.join(outpath, filename)
            df = sort_df(df, key='concept', custom_column_order=custom_column_order)
            df.to_csv(path, index=False, encoding='utf8')


@attr.s
class EntityIngredient(Ingredient):
    dtype = 'entities'

    def __attrs_post_init__(self):
        if isinstance(self.key, list):
            if len(self.key) != 1:
                raise IngredientError('entity ingredient must have only one primary key')
            self.key = self.key[0]

    def get_data(self) -> Dict[str, pd.DataFrame]:
        logger.info('evaluating data for {}'.format(self.id))
        if self.data_computed is not None:
            return self.data_computed

        ingredient_type = self.ingredient_type
        if ingredient_type == 'ddf':
            self.data_computed = self.get_data_from_ddf_dataset(self.dataset_path, self.key, self.value, self.row_filter)
        if ingredient_type == 'external':
            self.data_computed = self.get_data_from_external_csv(self.data, self.key, self.row_filter)
        if ingredient_type == 'inline':
            self.data_computed = self.get_data_from_inline_data(self.data, self.key, self.row_filter)

        return self.data_computed

    @staticmethod
    def get_data_from_ddf_dataset(dataset_path, key, value, row_filter):
        ddf = read_local_ddf(dataset_path)
        if ddf.concepts[key].concept_type == 'entity_domain':
            domain = key
            eset = None
        else:
            domain = ddf.concepts[key].props['domain']
            eset = key
        df = pd.DataFrame.from_records(ddf.entities[domain].to_dict(eset))
        if value != '*':
            if isinstance(value, Mapping):
                assert len(value) == 1
                assert list(value.keys())[0] in ['$in', '$nin']
                kw = list(value.keys())[0]
                if kw == '$in':
                    cols = [key, *value[kw]]
                    data = {key: df[cols]}
                else:
                    cols = [key, *df.columns.drop(value[kw])]
                    data = {key: df[cols]}
            else:
                cols = [key, *value]
                data = {key: df[cols]}
        else:
            data = {key: df}

        data = EntityIngredient.filter_row(data, row_filter)
        return data

    @staticmethod
    def get_data_from_external_csv(file_path, key, row_filter):
        df = pd.read_csv(file_path)
        data = {key: df}
        data = EntityIngredient.filter_row(data, row_filter)
        return data

    @staticmethod
    def get_data_from_inline_data(data, key, row_filter):
        df = pd.DataFrame.from_records(data)
        data = {key: df}
        data = EntityIngredient.filter_row(data, row_filter)
        return data

    def serve(self, outpath, **options):
        if not self.data_computed:
            self.get_data()
        sets = []
        no_keep_sets = options.get('no_keep_sets', False)  # serve as entity domain
        subpath = options.get('path', None)
        outpath = _handel_subpath(outpath, subpath)
        custom_column_order = options.get('custom_column_order', None)
        logger.info('serving: {}'.format(self.id))
        for k, df in self.data_computed.items():
            # handling is-- headers
            for c in df.columns:
                # changing to upper case is no longer required because it will be upper case string from DDFcsv
                # if df.dtypes[c] == 'bool':  # inferred boolean values
                #     df[c] = df[c].map(lambda x: str(x).upper() if not pd.isnull(x) else x)
                if c.startswith('is--'):  # is-- columns
                    if no_keep_sets:
                        df = df.drop(c, axis=1)
                    else:
                        sets.append(c[4:])
                        # df[c] = df[c].map(lambda x: str(x).upper() if not pd.isnull(x) else x)
            domain = self.key
            if k == domain:
                if len(sets) == 0:
                    path = os.path.join(outpath, 'ddf--entities--{}.csv'.format(k))
                    df = sort_df(df, key=domain, custom_column_order=custom_column_order)
                    df.to_csv(path, index=False, encoding='utf8')
                else:
                    for s in sets:
                        path = os.path.join(outpath, 'ddf--entities--{}--{}.csv'.format(k, s))
                        col = 'is--' + s
                        df_ = df[df[col] == 'TRUE'].dropna(axis=1, how='all')
                        if df_.empty:
                            logger.warning("empty dataframe for %s, not serving", str(s))
                            continue
                        df_ = df_.loc[:, lambda x: ~x.columns.str.startswith('is--')].copy()
                        df_[col] = 'TRUE'
                        df_ = df_.rename({k: s}, axis=1)  # use set name as primary key column name
                        df_ = sort_df(df_, key=s, custom_column_order=custom_column_order)
                        df_.to_csv(path, index=False, encoding='utf8')
                    # serve entities not in any sets
                    is_headers = list(map(lambda x: 'is--' + x, sets))
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
                df = sort_df(df, key=k, custom_column_order=custom_column_order)
                df.to_csv(path, index=False, encoding='utf8')


@attr.s
class DataPointIngredient(Ingredient):
    dtype = 'datapoints'

    def __attrs_post_init__(self):
        if isinstance(self.key, str):
            self.key = key_to_list(self.key)

    def get_data(self) -> Dict[str, dd.DataFrame]:
        logger.info('evaluating data for {}'.format(self.id))
        if self.data_computed is not None:
            return self.data_computed

        ingredient_type = self.ingredient_type
        if ingredient_type == 'ddf':
            self.data_computed = self.get_data_from_ddf_dataset(self.id, self.dataset_path, self.key,
                                                                self.value, self.row_filter)
        if ingredient_type == 'external':
            self.data_computed = self.get_data_from_external_csv(self.data, self.key, self.row_filter)
        if ingredient_type == 'inline':
            self.data_computed = self.get_data_from_inline_data(self.data, self.key, self.row_filter)

        return self.data_computed

    @classmethod
    def from_procedure_result(cls, id, key, data_computed: dict):
        if isinstance(key, str):
            key = key_to_list(key)
        res = cls(id, key)
        for _, df in data_computed.items():
            for k in key:
                assert k in df.columns, "the key {} not in data!".format(k)
        res.data_computed = create_dsk(data_computed)
        res.is_procedure_result = True
        return res

    @staticmethod
    def get_data_from_ddf_dataset(id, dataset_path, key, value, row_filter):
        data = dict()
        ddf = read_local_ddf(dataset_path)
        ddf_id = ddf.props.get('name', dataset_path)

        if value == '*':
            for i in ddf.indicators(by=key):
                data[i] = ddf.get_datapoints(i, by=key).data
        else:
            if isinstance(value, Sequence):  # just a list of indicators to include
                for i in value:
                    if i in ddf.indicators(by=key):
                        data[i] = ddf.get_datapoints(i, by=key).data
                    else:
                        logger.warning("indicator {} not found in dataset {}".format(i, ddf_id))
            else:  # a dictionary with queries
                assert len(value) == 1
                assert list(value.keys())[0] in ['$in', '$nin']
                for keyword, items in value.items():
                    if keyword == '$in':
                        for i in items:
                            matches = fnmatch.filter(ddf.indicators(by=key), i)
                            if len(matches) == 0:
                                logger.warning("indicator matching {} not found in dataset {}".format(i, ddf_id))
                                continue
                            for m in matches:
                                data[m] = ddf.get_datapoints(m, by=key).data
                    else:
                        all_indicators = ddf.indicators(by=key)
                        matches = set(all_indicators) - set(fnmatch.filter(all_indicators, items[0]))
                        for i in items[1:]:
                            matches = matches - set(fnmatch.filter(list(matches), i))
                        if len(matches) == 0:
                            logger.warning("indicators matching the value descriptor not "
                                           "found in dataset " + ddf_id)
                            continue
                        for m in matches:
                            data[m] = ddf.get_datapoints(m, by=key).data
        if len(data) == 0:
            raise IngredientError('no datapoint found for the ingredient: ' + id)

        data = DataPointIngredient.filter_row(data, row_filter)

        return data

    @staticmethod
    def get_data_from_external_csv(file_path, key, row_filter):
        df = pd.read_csv(file_path)
        data = {}
        df = df.set_index(key)
        for c in df.columns:
            data[c] = df[c]
        data = DataPointIngredient.filter_row(data, row_filter)
        data = create_dsk(data, parts=1)
        return data

    @staticmethod
    def get_data_from_inline_data(data, key, row_filter):
        df = pd.DataFrame.from_records(data)
        data = {}
        df = df.set_index(key)
        for c in df.columns:
            data[c] = df[[c]].reset_index()
        data = DataPointIngredient.filter_row(data, row_filter)
        data = create_dsk(data, parts=1)
        return data

    def compute(self) -> Dict[str, pd.DataFrame]:
        """return a pandas dataframe version of self.data"""
        new_data = dict()
        if not self.data_computed:
            self.data_computed = self.get_data()
        for k, df in self.data_computed.items():
            new_data[k] = df.compute()
        return new_data

    def serve(self, outpath, **options):
        digits = read_opt(options, 'digits', default=5)
        dont_serve_empty = read_opt(options, 'drop_empty_datapoints', default=True)
        split_by = read_opt(options, 'split_datapoints_by', default=False)
        subpath = read_opt(options, 'path', default=None)
        custom_key_order = read_opt(options, 'custom_key_order', default=None)
        outpath = _handel_subpath(outpath, subpath)

        logger.info('serving: {}'.format(self.id))

        def to_disk(df_input, k, path, order=None):
            df = df_input.copy()
            if not np.issubdtype(df[k].dtype, np.number):
                try:
                    df[k] = df[k].astype(float)
                    df[k] = df[k].map(lambda x: format_float_digits(x, digits))
                except ValueError:
                    logger.warning("data not numeric: " + k)
            else:
                df[k] = df[k].map(lambda x: format_float_digits(x, digits))
            # sort if custom key order defined
            if order:
                indicator_col, = set(df.columns) - set(order)
                new_cols = [*order, indicator_col]
                df = df[new_cols]
            df.to_csv(path, encoding='utf8', index=False)

        # check if custom key match indicator key
        if custom_key_order:
            if set(custom_key_order) != set(self.key):
                logger.warning("custom keys are not same as ingredient keys")
                logger.warning("ignoring custom_key_order setting")
                custom_key_order = None

        # compute all dask dataframe to pandas dataframe and save to csv files
        # TODO: maybe no need to convert to pandas?
        for k, df in self.compute().items():
            if df.empty:
                logger.info('empty data for {}'.format(k))
                if dont_serve_empty:
                    continue
            by = self.key
            df = sort_df(df, key=by, sort_key_columns=False)
            if not split_by:
                columns = [*by, k]
                path = os.path.join(
                    outpath,
                    'ddf--datapoints--{}--by--{}.csv'.format(k, '--'.join(by)))
                to_disk(df[columns], k, path, custom_key_order)
            else:
                # split datapoints by entities. Firstly we calculate all possible
                # combinations of entities, and then filter the dataframe, create
                # file names and save them into disk.
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
                        value_ = str(value)
                        entity_strings.append(entity + '-' + value_)
                        if len(query) > 0:
                            query = query + " and " + "{} == '{}'".format(entity, value_)
                        else:
                            query = "{} == '{}'".format(entity, value_)

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
                    # logger.debug('query is: ' + query)
                    df_part = df.query(query)
                    to_disk(df_part[columns], k, path, custom_key_order)


@attr.s
class SynonymIngredient(Ingredient):
    dtype = 'synonyms'

    def __attrs_post_init__(self):
        if isinstance(self.key, str):
            self.key = key_to_list(self.key)

    def get_data(self):
        logger.info('evaluating data for {}'.format(self.id))
        if self.data_computed is not None:
            return self.data_computed

        ingredient_type = self.ingredient_type
        if ingredient_type == 'ddf':
            self.data_computed = self.get_data_from_ddf_dataset(self.dataset_path, self.key)
        if ingredient_type == 'external':
            self.data_computed = self.get_data_from_external_csv(self.data, self.key)
        if ingredient_type == 'inline':
            self.data_computed = self.get_data_from_inline_data(self.data, self.key)

        return self.data_computed

    @staticmethod
    def get_data_from_ddf_dataset(dataset_path, key):
        ddf = read_local_ddf(dataset_path)
        key.remove('synonym')
        k = key[0]
        synonyms = ddf.get_synonyms(k).to_dict()
        # nothing to do with `value` and `row_filter`.
        return synonyms

    @staticmethod
    def get_data_from_external_csv(file_path, key):
        df = pd.read_csv(file_path)
        key.remove('synonym')
        k = key[0]
        data = {k: df.set_index('synonym')[k].to_dict()}
        # nothing to do with `value` and `row_filter`.
        return data

    @staticmethod
    def get_data_from_inline_data(data, key):
        df = pd.DataFrame.from_records(data)
        key.remove('synonym')
        k = key[0]
        data = {k: df.set_index('synonym')[k].to_dict()}
        # nothing to do with `value` and `row_filter`.
        return data

    def serve(self, *args, **kwargs):
        raise NotImplementedError


# functions for creating Ingredient from recipe definitions

def key_to_list(key):
    """make a list that contains primaryKey of this ingredient"""
    return [x.strip() for x in key.split(',')]


def infer_type_from_keys(keys: list):
    """infer ddf data type from the primary key"""
    if len(keys) == 1:
        if keys[0] == 'concept':
            return 'concepts'
        else:
            return 'entities'
    if 'synonym' in keys:
        return 'synonyms'
    else:
        return 'datapoints'


def ingredient_from_dict(dictionary: dict, **chef_options) -> Ingredient:
    """create ingredient from recipe definition and options. Parameters
    for ingredient should be passed in a dictionary. See the doc for
    :ref:`ingredient def` or :py:class:`ddf_utils.chef.model.ingredient.Ingredient` for
    available parameters.
    """
    ingred_id = read_opt(dictionary, 'id', default=None, method='pop')
    dataset = read_opt(dictionary, 'dataset', default=None, method='pop')
    data = read_opt(dictionary, 'data', default=None, method='pop')
    key = read_opt(dictionary, 'key', required=True, method='pop')
    value = read_opt(dictionary, 'value', default='*', method='pop')
    row_filter = read_opt(dictionary, 'filter', default=None, method='pop')

    dataset_dir = read_opt(chef_options, 'ddf_dir', default='./')
    external_csv_dir = read_opt(chef_options, 'external_csv_dir', default='./')

    if ((dataset is None and data is None) or
            (dataset is not None and data is not None)):
        raise ValueError("Please provide either `dataset` or `data` for {}".format(ingred_id))

    if ingred_id is None:
        ingred_id = gen_sym('inline', None, dictionary)

    if dataset is not None:
        ingredient_type = 'ddf'
    else:
        if isinstance(data, str):
            ingredient_type = 'external'
        else:
            ingredient_type = 'inline'

    if len(dictionary.keys()) > 0:
        logger.warning("Ignoring following keys: {}".format(list(dictionary.keys())))

    if ingredient_type == 'ddf':
        if is_url(dataset):  # data will read from github dataset
            repo = Repo(dataset, base_path=dataset_dir)  # this will clone the repo to dataset_dir if it doesn't exist
            dataset = os.path.relpath(repo.local_path, dataset_dir)
        # else:
        #     dataset = os.path.join(dataset_dir, dataset)

    if ingredient_type == 'external':
        data = os.path.join(external_csv_dir, data)

    keys = key_to_list(key)
    dtype = infer_type_from_keys(keys)
    if dtype == 'concepts':
        return ConceptIngredient(id=ingred_id, dataset=dataset, data=data, key=key, value=value,
                                 row_filter=row_filter, base_dir=dataset_dir)
    if dtype == 'entities':
        return EntityIngredient(id=ingred_id, dataset=dataset, data=data, key=key, value=value,
                                row_filter=row_filter, base_dir=dataset_dir)
    if dtype == 'datapoints':
        return DataPointIngredient(id=ingred_id, dataset=dataset, data=data, key=key, value=value,
                                   row_filter=row_filter, base_dir=dataset_dir)
    if dtype == 'synonyms':
        return SynonymIngredient(id=ingred_id, dataset=dataset, data=data, key=key, value=value,
                                 row_filter=row_filter, base_dir=dataset_dir)


def get_ingredient_class(cls):
    d = {
        'concepts': ConceptIngredient,
        'entities': EntityIngredient,
        'datapoints': DataPointIngredient,
        'synonyms': SynonymIngredient
    }
    return d[cls]


def _handel_subpath(outpath, subpath):
    if subpath is None:
        newpath = outpath
    else:
        newpath = os.path.join(outpath, subpath)

    os.makedirs(newpath, exist_ok=True)

    return newpath
