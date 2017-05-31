# -*- coding: utf-8 -*-

"""new version of ddf_reader, base on datapackage.json"""

import os
import pandas as pd
from . import config
from .datapackage import get_datapackage
from pprint import pformat


# main class for ddf reading
class DDF():
    """DDF reader class

    The reader instance accepts an dataset id or absolute path on init. If absolute path is given,
    it will load the dataset in path. Other wise it will search the dataset id in the path set by
    the `DDF_SEARCH_PATH` global variable.
    """
    def __init__(self, ddf_id, no_check_valid=False):
        if os.path.isabs(ddf_id):
            self.dataset_path = ddf_id
            self.ddf_id = os.path.dirname(ddf_id)
        else:
            self.dataset_path = os.path.join(config.DDF_SEARCH_PATH, ddf_id)
            self.ddf_id = ddf_id
        if not no_check_valid:
            assert is_dataset(self.dataset_path), \
                "path is not ddf dataset: {}".format(self.dataset_path)
        self._datapackage = None
        self._concepts = None

    def __repr__(self):
        return "dataset {}".format(self.ddf_id)

    def describe(self):
        return ("concepts in this dataset: \n\n{}"
                .format(pformat(self.concepts['concept_type'].to_dict())))

    @property
    def datapackage(self):
        """the datapackage object. create one if it doesn't exist"""
        if not self._datapackage:
            self._datapackage = get_datapackage(self.dataset_path, update=False)
        return self._datapackage

    @property
    def concepts(self):
        """convenient function to get all concepts"""
        if self._concepts is None:
            self._concepts = self.get_concepts()
        return self._concepts

    @property
    def dtypes(self):
        """return a mapping for column -> python object type. internal use only."""
        concept_types = self.concepts['concept_type']
        res = dict()
        for c, v in concept_types.iteritems():
            if v in ['string', 'entity_domain', 'entity_set']:
                res[c] = str
            elif v in ['time']:
                res[c] = int  # TODO: support more time format
            else:  # for measures and other concept type, let pandas decide
                continue

        return res

    @property
    def indicator_dict(self):
        """return all indicators"""
        return dict([name, list(item.keys())]
                    for name, item in self.get_datapoint_files().items())

    def get_all_files(self):
        """return a list of all files in this dataset"""
        resources = self.datapackage['resources']
        return [x['path'] for x in resources]

    def get_concept_files(self):
        """return a list of all concept files"""
        resources = self.datapackage['resources']
        return [x['path'] for x in resources if x['schema']['primaryKey'] == 'concept']

    def get_concepts(self, concept_type='all', **kwargs):
        """get concepts from the concept table

        Args
        ----
        concept_type : `str` or `list`
            the concept type(s) to filter, if 'all' is provided, all concepts will be returned
        **kwargs : dict
            keyword arguments provided to :py:func:`pandas.read_csv`

        Returns
        -------
        DataFrame
            concept table

        """
        concept_files = self.get_concept_files()
        all_concepts = pd.concat([
                pd.read_csv(os.path.join(self.dataset_path, x),
                            index_col='concept', **kwargs) for x in concept_files])
        if concept_type == 'all':
            return all_concepts
        elif isinstance(concept_type, str):
            return all_concepts[all_concepts.concept_type == concept_type]
        elif isinstance(concept_type, list):
            return all_concepts[all_concepts.concept_type.isin(concept_type)]

    def get_entities(self, domain=None, **kwargs):
        """get entities from one or all domains

        Args
        ----
        domain : `str`, optional
            the domain to filter, if not provided, all domain will be returned
        **kwargs : dict
            keyword arguments provided to :py:func:`pandas.read_csv`

        Returns
        -------
        dict
            a dictionary like ``{entity_set_name : DataFrame}``

        """
        resources = self.datapackage['resources']
        entity_concepts = self.get_concepts(['entity_domain', 'entity_set'])

        if domain:
            entity_concepts = entity_concepts.reset_index()
            if 'domain' in entity_concepts.columns:
                mask = ((entity_concepts.domain == domain) |
                        ((entity_concepts.concept == domain) &
                         (entity_concepts.concept_type == 'entity_domain')))
            else:
                mask = entity_concepts.concept == domain
            entity_concepts = entity_concepts[mask].set_index('concept')

        if 'dtype' in kwargs.keys():
            dtype = kwargs.pop('dtype')
        else:
            dtype = self.dtypes

        entities = dict()
        for res in resources:
            key = res['schema']['primaryKey']
            if isinstance(key, str) and key != 'concept':
                name = res['name'].split('--')[-1]  # TODO: don't judge by name
                if (res['schema']['primaryKey'] in list(entity_concepts.index) or
                    (domain is not None and res['schema']['primaryKey'] == domain)):
                    entities[name] = pd.read_csv(
                        os.path.join(self.dataset_path, res['path']),
                        dtype=dtype, **kwargs)
                    entities[name] = entities[name].set_index(res['schema']['primaryKey'])
        return entities

    def get_datapoint_files(self):
        """return a list of datapoints files"""
        datapoints = dict()
        resources = self.datapackage['resources']

        for res in resources:
            key = res['schema']['primaryKey']
            if isinstance(key, list):
                key = tuple([x for x in key])
                fields = [x['name'] for x in res['schema']['fields']]
                name = [x for x in fields if x not in key]
                assert len(name) == 1
                name = name[0]
                if name in datapoints.keys():
                    if key in datapoints[name].keys():
                        datapoints[name][key].append(res['path'])
                    else:
                        datapoints[name][key] = [res['path']]
                else:
                    datapoints[name] = {key: [res['path']]}
        return datapoints

    def __build_datapoint_df(self, files):
        return pd.concat(
            [pd.read_csv(os.path.join(self.dataset_path, x), dtype=self.dtypes) for x in files],
            ignore_index=True
        )

    def get_datapoints(self, measure=None, primaryKey=None):
        """get datapoints, filter by concept or primaryKey

        Args
        ----
        measure : `str`, optional
            only get this measure
        primaryKey : `str`, optional
            only get this primaryKey

        Returns
        -------
        dict
            A dictionary like ``{ measure_name : { primaryKeys : DataFrame }}``
        """
        datapoint_files = self.get_datapoint_files()
        datapoints = dict()

        if measure:
            if primaryKey:  # both measure and primaryKey given
                datapoints[measure] = {
                    primaryKey: (self.__build_datapoint_df(datapoint_files[measure][primaryKey])
                                 .set_index(list(primaryKey)))
                }
            else:  # only measure given
                datapoints[measure] = dict([
                        (k, self.__build_datapoint_df(
                                datapoint_files[measure][k]).set_index(list(k)))
                        for k in datapoint_files[measure].keys()])
        else:
            if primaryKey:  # only primaryKey given
                for m in datapoint_files.keys():
                    datapoints[m] = {
                        primaryKey: (self.__build_datapoint_df(datapoint_files[m][primaryKey])
                                     .set_index(list(primaryKey)))
                    }
            else:  # no parameters, return all
                for m in datapoint_files.keys():
                    datapoints[m] = dict([
                            (k, self.__build_datapoint_df(
                                    datapoint_files[m][k]).set_index(list(k)))
                            for k in datapoint_files[m].keys()])
        return datapoints

    def get_datapoint_df(self, measure, primaryKey=None):
        """get datapoints by measure, returns a DataFrame

        Args
        ----
        measure : `str`
            the measure to get
        primaryKey : `str`, optional
            the primaryKey to get, if not provided and the datapoint have multiple
            primaryKeys avaliable, the first primaryKey in datapackage will be returned

        """
        datapoint_files = self.get_datapoint_files()

        if len(datapoint_files[measure].keys()) == 1:
            keys = list(datapoint_files[measure].keys())[0]
            if primaryKey:
                if not set(keys) == set(primaryKey):
                    raise KeyError('no such key for the measure: ', primaryKey)
            df = self.get_datapoints(measure, keys)
            return df[measure][keys]
        else:
            if not primaryKey:
                raise ValueError("please specify a primaryKey for measures with multiple "
                                 "primaryKeys")
            for keys in datapoint_files[measure].keys():
                if set(keys) == set(primaryKey):
                    df = self.get_datapoints(measure, keys)
                    return df[measure][keys]
            raise ValueError('key not found for the measure!')


# helper functions:
# check if a directory is dataset root dir
def is_dataset(path):
    """check if a directory is a dataset directory

    This function checks if ddf--index.csv and datapackage.json exists
    to judge if the dir is a dataset.
    """
    index_path = os.path.join(path, 'ddf--index.csv')
    datapackage_path = os.path.join(path, 'datapackage.json')
    if os.path.exists(index_path) or os.path.exists(datapackage_path):
        return True
    else:
        return False


# function for listing all ddf projects
def list_datasets():
    """list all availabile datasets"""
    datasets = []
    for d in next(os.walk(config.DDF_SEARCH_PATH))[1]:
        dataset_path = os.path.join(config.DDF_SEARCH_PATH, d)
        if is_dataset(dataset_path):
            datasets.append(d)
    return datasets
