# -*- coding: utf-8 -*-

"""datapackage model"""

import json
import logging
import os
import os.path as osp
from pathlib import Path

from collections import Mapping, Sequence
from itertools import product

import dask.dataframe as dd
from dask import delayed

import pandas as pd
from tqdm import tqdm

from .ddf import Dataset
from .utils import load_datapackage_json

logger = logging.getLogger('root')


class Resource:
    """A base class for all resources in a CSV datapackage.

    We assume all recources are CSV files here.
    """
    def __init__(self, path, name, fields, primaryKey, base_dir='./'):
        # TODO: find out all possible keys
        self.path = path
        self.name = name
        self.fields = fields
        self.primaryKey = primaryKey
        self.base_dir = base_dir

    def __repr__(self):
        return "name: {}, key: {}".format(self.name, self.primaryKey)

    @classmethod
    def from_datapackage_record(cls, r, base_dir=None):
        path = r['path']
        name = r['name']
        fields = r['schema']['fields']
        primaryKey = r['schema']['primaryKey']

        if base_dir is None:
            return Resource(path, name, fields, primaryKey, './')
        else:
            return Resource(path, name, fields, primaryKey, base_dir)

    def to_datapackage_record(self):
        rec = dict()
        rec['path'] = self.path
        rec['name'] = self.name
        rec['schema'] = {'primaryKey': self.primaryKey, 'fields': self.fields}
        return rec

    @property
    def full_path(self):
        # return Path(self.base_dir, self.path)
        # dask don't support Path object yet so we return a path string.
        return osp.join(self.base_dir, self.path)


class Datapackage:
    def __init__(self, datapackage, base_dir='./'):
        """create datapackage object from datapackage descriptor.

        datapackage: can be a path to datapackage file or dictioinary in datapackage format
        """
        # TODO: change this to `from_datapackage_json`
        if isinstance(datapackage, Mapping):
            self.base_dir = base_dir
            self.datapackage = datapackage
        elif isinstance(datapackage, str):
            self.base_dir, self.datapackage = load_datapackage_json(datapackage)

        # for caching
        self._resources = None
        self._concepts = None
        self._entities = None
        self._synonyms = None
        self._dataset = None

        # config for read_csv
        self._default_reader_options = {'keep_default_na': False, 'na_values': ['']}

    @property
    def name(self):
        return self.datapackage['name']

    @property
    def resources(self):
        if self._resources is None:
            self._resources = [Resource.from_datapackage_record(r, base_dir=self.base_dir)
                               for r in self.datapackage['resources']]
        return self._resources

    @property
    def concepts_resources(self):
        return [r for r in self.resources if r.primaryKey == 'concept']

    @property
    def entities_resources(self):
        return [r for r in self.resources if
                (r.primaryKey != 'concept') and (isinstance(r.primaryKey, str))]

    @property
    def datapoints_resources(self):
        # TODO: it might not be true that primaryKey is always a list for datapoints
        # sometimes it could be just one string?
        return [r for r in self.resources
                if isinstance(r.primaryKey, list) and 'synonym' not in r.primaryKey]

    @property
    def synonyms_resources(self):
        return [r for r in self.resources
                if isinstance(r.primaryKey, list) and 'synonym' in r.primaryKey]

    @property
    def index_table(self):
        rows = list()

        for r in self.resources:
            pkey = r.primaryKey
            if not isinstance(pkey, list):
                pkey = [pkey]
            pkey = sorted(pkey)
            columns = [x['name'] for x in r.fields]

            if len(columns) == len(pkey):  # there is only the primary column.
                row = dict()
                row['pkey'] = ','.join(pkey)
                row['column'] = None
                row['path'] = r.full_path
                row['name'] = r.name
                rows.append(row)
                continue

            for c in columns:
                if c in pkey:
                    continue
                row = dict()
                row['pkey'] = ','.join(pkey)
                row['column'] = c
                row['path'] = r.full_path
                row['name'] = r.name
                rows.append(row)
        return pd.DataFrame.from_records(rows)

    @property
    def dataset(self):
        if self._dataset is None:
            self._dataset = self.load()
        return self._dataset

    @property
    def concepts(self):
        """return a DataFrame for concepts"""
        if self._concepts is None:
            fs = (self.index_table[self.index_table.pkey == 'concept']['path']
                  .unique().tolist())
            concepts = [pd.read_csv(f, **self._default_reader_options) for f in fs]
            self._concepts = pd.concat(concepts, ignore_index=True)
        return self._concepts

    @property
    def entities(self):
        """return dictionary, which keys are domains and values are DataFrames"""
        if self._entities is None:
            cdf = self.concepts
            idx_table = self.index_table
            entities = dict()
            for domain in cdf[cdf['concept_type'] == 'entity_domain']['concept']:
                entities[domain] = list()
                if 'domain' in cdf.columns:
                    esets = cdf[(cdf['concept_type'] == 'entity_set') &
                                (cdf['domain'] == domain)]['concept']
                    if not esets.empty:
                        rows = (idx_table[(idx_table.pkey == domain) |
                                          (idx_table.pkey.isin(esets.tolist()))]
                                .drop_duplicates(subset='path'))
                        # filter ambiougus names where domain is different but set name
                        # is same.
                        for _, r in rows.iterrows():
                            if domain not in r['name']:
                                continue
                            df = pd.read_csv(r.path, dtype=str, **self._default_reader_options)
                            if r.pkey != domain:
                                df = df.rename(columns={r.pkey: domain})
                            entities[domain].append(df)
                        entities[domain] = pd.concat(entities[domain], ignore_index=True)
                    else:  # no sets for this domain
                        paths = (idx_table[idx_table.pkey == domain]['path']
                                 .unique().tolist())
                        entities[domain] = dd.read_csv(paths, dtype=str, **self._default_reader_options).compute()
                else:  # no domain column
                    paths = (idx_table[idx_table.pkey == domain]['path']
                             .unique().tolist())
                    entities[domain] = dd.read_csv(paths, dtype=str, **self._default_reader_options).compute()

            self._entities = entities
        return self._entities

    @property
    def synonyms(self):
        """return a dictonary, where keys are domains or `concept` and values are DataFrames"""
        if self._synonyms is not None:
            return self._synonyms

        syms = dict()
        for r in self.synonyms_resources:
            pks = r.primaryKey
            key_for_sym = [x for x in pks if x != 'synonym']
            assert len(key_for_sym) == 1, "synonyms resource can only have two primary keys"
            syms[key_for_sym[0]] = pd.read_csv(r.full_path, dtype=str, **self._default_reader_options)
        self._synonyms = syms
        return self._synonyms

    def indicators(self, by=None):
        """return a list of indicator names, filtered by the primaryKeys"""
        res = set()
        for r in self.datapoints_resources:
            if by is not None and set(by) != set(r.primaryKey):
                continue
            for f in r.fields:
                if f['name'] not in r.primaryKey:
                    res.add(f['name'])
        return list(res)

    def get_datapoint_df(self, i, primary_key):
        if isinstance(primary_key, Sequence):
            pkey = ','.join(sorted(primary_key))
            dtypes = dict([x, 'category'] for x in primary_key)
        else:
            pkey = primary_key
            dtypes = {primary_key: 'category'}

        paths = (self.index_table[(self.index_table.pkey == pkey) &
                                  (self.index_table.column == i)]['path']
                 .unique().tolist())
        if len(paths) == 0:
            raise ValueError('no datapoint find for {} by {}'.format(i, pkey))

        cdf = self.concepts
        time_concepts = cdf[cdf['concept_type'] == 'time']['concept'].values

        for tc in time_concepts:
            dtypes[tc] = 'int16'

        return dd.read_csv(paths, dtype=dtypes, **self._default_reader_options)

    def get_entity(self, entity_domain, entity_set=None):
        df = self.entities[entity_domain]
        if entity_set is not None:
            col = 'is--{}'.format(entity_set)
            df = (df[df[col] == 'TRUE'].dropna(axis=1, how='all'))
            # rename the primaryKey column to match the entity set name
            df = df.rename(columns={entity_domain: entity_set})
        return df

    def get_synonym_dict(self, concept):
        """return a synonym dictionary for a concept"""
        try:
            df = self.synonyms[concept]
        except KeyError:  # no synonyms for this concept
            return None
        return df.set_index('synonym')[concept].to_dict()

    # TODO: refactor this method
    def load(self, **kwargs):
        """read from local DDF csv dataset.

        datapackage: path to the datapackage folder or datapackage.json

        Keyword Args
        ============
        no_datapoints : bool
            if true, return only first few rows of a datapoints dataframe, to speedup things.
        """
        logging.info("loading dataset from disk: " + self.name)

        concepts = list()
        entities_ = list()  # temp, the final data will be entities
        entities = dict()
        datapoints = dict()

        def _update_datapoints(fn_, keys_, indicator_name_):
            """helper function to make datapoints dictionary"""
            if keys_ in datapoints.keys():
                if indicator_name_ in datapoints[keys_]:
                    datapoints[keys_][indicator_name_].append(fn_)
                else:
                    datapoints[keys_][indicator_name_] = [fn_]
            else:
                datapoints[keys_] = dict()
                datapoints[keys_][indicator_name_] = [fn_]

        no_datapoints = kwargs.get('no_datapoints', False)

        dp = self.datapackage

        # concepts
        for r in self.resources:
            pkey = r.primaryKey
            if pkey == 'concept':
                concepts.append(pd.read_csv(r.full_path, **self._default_reader_options))

        concepts = pd.concat(concepts)

        time_concepts = concepts[concepts['concept_type'] == 'time'].concept.values

        # others
        for r in self.resources:
            pkey = r.primaryKey
            if pkey == 'concept':
                continue
            elif isinstance(pkey, str):  # entities
                entities_.append(
                    {
                        # "data": pd.read_csv(osp.join(base_dir, r['path']), dtype={pkey: str}),
                        "data": pd.read_csv(r.full_path, dtype=str, encoding='utf8',  # read all as string
                                            **self._default_reader_options),
                        "key": pkey
                    })
            elif 'synonym' not in r.primaryKey:  # datapoints
                assert not isinstance(pkey, str)
                fn = r.full_path
                df = next(pd.read_csv(fn, chunksize=1, **self._default_reader_options))
                indicator_names = list(set(df.columns) - set(pkey))

                if len(indicator_names) == 0:
                    raise ValueError('No indicator in {}'.format(r.path))

                keys = tuple(sorted(pkey))
                # TODO: if something went wrong, such as there is a typo in pkey, give better message
                if len(indicator_names) == 1:
                    indicator_name = indicator_names[0]
                    _update_datapoints(fn, keys, indicator_name)
                else:
                    for indicator_name in indicator_names:
                        _update_datapoints(fn, keys, indicator_name)
            else:  # synonyms/other types.
                continue

        # datapoints
        for i, kvs in datapoints.items():
            for k, l in kvs.items():
                dtypes = dict([x, 'category'] for x in i)
                for tc in time_concepts:
                    # dtypes[tc] = 'uint16'  # not using this because of bug #96
                    dtypes[tc] = 'int16'  # TODO: maybe there are other time format?
                cols = list(i + tuple([k]))

                if not no_datapoints:
                    df = dd.read_csv(l, dtype=dtypes, **self._default_reader_options)[cols]
                else:
                    df = pd.DataFrame([], columns=cols)

                kvs[k] = df
        # entities
        # TODO: check if concept_type match the type inferred from file.
        # i.e. it's wrong when concept file says a concept is domain but
        # the ddf file indicates it's entity set.
        if 'entity_set' not in concepts.concept_type.values:  # only domains
            for e in entities_:
                entities[e['key']] = [e['data']]
        else:
            for domain in concepts[concepts.concept_type == 'entity_domain']['concept']:
                entities[domain] = []
                for e in entities_:
                    if e['key'] == domain:
                        entities[domain].append(e['data'])
                    elif e['key'] in concepts[concepts.domain == domain]['concept'].values:
                        e['data'] = e['data'].rename(columns={e['key']: domain})
                        entities[domain].append(e['data'])
        for e, l in entities.items():
            try:
                df = l[0]
            except IndexError:
                logging.critical('no entity file found for {}!'.format(e))
                raise
            for df_ in l[1:]:
                df = pd.merge(df, df_, on=e, how='outer')
                for c in list(df.columns):
                    if c.endswith('_x'):
                        c_orig = c[:-2]
                        df[c_orig] = None
                        for i in df.index:
                            if not pd.isnull(df.loc[i, c]):
                                if not pd.isnull(df.loc[i, c_orig+'_y']):
                                    # assert df.loc[i, c] == df.loc[i, c_orig+'_y'], \
                                    #     "different values for same cell:{}, {}".format(i, c)
                                    logger.debug('different values for same cell: '
                                                 '{}: {}, {}'.format(c_orig,
                                                                     df.at[i, c_orig],
                                                                     df.at[i, c_orig+'_y']))
                                    df.loc[i, c_orig] = df.loc[i, c]
                                else:
                                    df.loc[i, c_orig] = df.loc[i, c]
                            else:
                                df.loc[i, c_orig] = df.loc[i, c_orig+'_y']
                        df = df.drop([c, c_orig+'_y'], axis=1)
            entities[e] = df

        return Dataset(concepts=concepts, entities=entities,
                       datapoints=datapoints, attrs=dp)

    def generate_ddfschema(self):
        ds = self.dataset
        cdf = ds.concepts.set_index('concept')
        hash_table = {}
        ddf_schema = {'concepts': [], 'entities': [], 'datapoints': [], 'synonyms': []}
        entity_value_cache = dict()

        # generate set-membership details for every single entity in dataset
        for domain, df in ds.entities.items():
            entity_value_cache[domain] = dict()
            for _, row in df.iterrows():
                sets = set()
                sets.add(domain)
                for c in df.columns:
                    if c.startswith('is--'):
                        if row[c] == "TRUE":
                            sets.add(c[4:])
                entity_value_cache[domain][row[domain]] = tuple(sets)

        all_entity_concepts = cdf[cdf.concept_type.isin(['entity_set', 'entity_domain'])].index
        dtypes = dict([(c, 'category') for c in all_entity_concepts])  # set all entity column to string type

        def _which_sets(entity_, domain_):
            try:
                return entity_value_cache[domain_][entity_]
            except KeyError:
                logging.debug('entity {} is not in {} domain!'.format(entity_, domain_))
                raise

        def _gen_key_value_object(resource):
            logging.debug('working on: {}'.format(resource.path))
            if isinstance(resource.primaryKey, str):
                pkeys = [resource.primaryKey]
            else:
                pkeys = resource.primaryKey

            entity_cols = [x for x in pkeys if
                           (x in cdf.index) and
                           (cdf.loc[x, 'concept_type'] in ['entity_set', 'entity_domain'])]
            value_cols = list(set([x['name'] for x in resource.fields]) - set(pkeys))

            data = pd.read_csv(resource.full_path, dtype=dtypes, **self._default_reader_options)

            # for resources that have entity_columns: only consider all permutations on entity columns
            if len(entity_cols) > 0:
                data = data[entity_cols].drop_duplicates()

            pkeys_prop = dict()
            for c in pkeys:
                if c not in cdf.index:
                    pkeys_prop[c] = {'type': 'non_concept'}
                elif cdf.at[c, 'concept_type'] == 'entity_set':
                    pkeys_prop[c] = {'type': 'entity_set',
                                     'domain': cdf.at[c, 'domain']}
                elif cdf.at[c, 'concept_type'] == 'entity_domain':
                    pkeys_prop[c] = {'type': 'entity_domain'}
                else:
                    pkeys_prop[c] = {'type': 'others'}

            all_permutations = set()
            for _, r in data.iterrows():
                perm = list()
                for c in pkeys:
                    if pkeys_prop[c]['type'] == 'entity_set':
                        domain = pkeys_prop[c]['domain']
                        perm.append(_which_sets(r[c], domain))
                    elif pkeys_prop[c]['type'] == 'entity_domain':
                        perm.append(_which_sets(r[c], c))
                    else:
                        perm.append(tuple([c]))

                all_permutations.add(tuple(perm))

            for row in all_permutations:
                for perm in product(*row):
                    if len(value_cols) > 0:
                        for c in value_cols:
                            yield {'primaryKey': list(perm), 'value': c, 'resource': resource.name}
                    else:
                        yield {'primaryKey': list(perm), 'value': None, 'resource': resource.name}

        def _add_to_schema(resource_schema):
            key = '-'.join(sorted(resource_schema['primaryKey']))
            if not pd.isnull(resource_schema['value']):
                hash_val = key + '--' + resource_schema['value']
            else:
                hash_val = key + '--' + 'nan'
            if hash_val not in hash_table.keys():
                hash_table[hash_val] = {
                    'primaryKey': sorted(resource_schema['primaryKey']),
                    'value': resource_schema['value'],
                    'resources': set([resource_schema['resource']])
                }
            else:
                hash_table[hash_val]['resources'].add(resource_schema['resource'])

        if logger.getEffectiveLevel() != 10:
            pbar = tqdm(total=len(self.resources))

        for g in map(_gen_key_value_object, self.resources):
            if logger.getEffectiveLevel() != 10:
                pbar.update(1)
            for kvo in g:
                logging.debug("adding kvo {}".format(str(kvo)))
                _add_to_schema(kvo)

        if logger.getEffectiveLevel() != 10:
            pbar.close()

        for sch in hash_table.values():
            sch['resources'] = list(sch['resources'])
            if len(sch['primaryKey']) == 1:
                if sch['primaryKey'][0] == 'concept':
                    ddf_schema['concepts'].append(sch)
                else:
                    ddf_schema['entities'].append(sch)
            else:
                if 'synonym' in sch['primaryKey']:
                    ddf_schema['synonyms'].append(sch)
                else:
                    ddf_schema['datapoints'].append(sch)

        self.datapackage['ddfSchema'] = ddf_schema

    def dump(self, path):
        """dump the datapackage to path."""
        # TODO: dump all files
        # for now we only dump the datapackage.json
        with open(osp.join(path, 'datapackage.json')) as f:
            json.dump(self.datapackage, f)
            f.close()
