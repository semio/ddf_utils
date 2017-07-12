# -*- coding: utf-8 -*-

"""datapackage model"""

import os
import os.path as osp
import json
import pandas as pd
import dask.dataframe as dd
from .ddf import Dataset
from itertools import product
from .utils import load_datapackage_json
from tqdm import tqdm
from collections import Mapping

import logging


class Datapackage:
    def __init__(self, datapackage, base_dir='./', dataset=None):
        """create datapackage object from datapackage descriptor.

        datapackage: can be a path to datapackage file or dictioinary in datapackage format
        """
        if isinstance(datapackage, Mapping):
            self.base_dir = base_dir
            self.datapackage = datapackage
        elif isinstance(datapackage, str):
            self.base_dir, self.datapackage = load_datapackage_json(datapackage)

        self._dataset = dataset

    @property
    def name(self):
        return self.datapackage['name']

    @property
    def resources(self):
        return self.datapackage['resources']

    @property
    def concepts_resources(self):
        return [r for r in self.resources if r['schema']['primaryKey'] == 'concept']

    @property
    def entities_resources(self):
        return [r for r in self.resources if
                (r['schema']['primaryKey'] != 'concept') and (isinstance(r['schema']['primaryKey'], str))]

    @property
    def datapoints_resources(self):
        return [r for r in self.resources if isinstance(r['schema']['primaryKey'], list)]

    @property
    def dataset(self):
        if self._dataset is None:
            self._dataset = self.load()

        return self._dataset

    def load(self, **kwargs):
        """read from local DDF csv dataset.

        datapackage: path to the datapackage folder or datapackage.json
        """
        logging.info("loading dataset from disk: " + self.name)

        concepts = list()
        entities_ = list()  # temp, the final data will be entities
        entities = dict()
        datapoints = dict()

        no_datapoints = kwargs.get('no_datapoints', None)

        base_dir, dp = self.base_dir, self.datapackage

        # concepts
        for r in dp['resources']:
            pkey = r['schema']['primaryKey']
            if pkey == 'concept':
                concepts.append(pd.read_csv(osp.join(base_dir, r['path'])))

        concepts = pd.concat(concepts)

        time_concepts = concepts[concepts['concept_type'] == 'time'].concept.values

        # others
        for r in dp['resources']:
            pkey = r['schema']['primaryKey']
            if pkey == 'concept':
                continue
            elif isinstance(pkey, str):  # entities
                entities_.append(
                    {
                        # "data": pd.read_csv(osp.join(base_dir, r['path']), dtype={pkey: str}),
                        "data": pd.read_csv(osp.join(base_dir, r['path']), dtype=str),  # read all as string
                        "key": pkey
                    })
            else:  # datapoints
                if not no_datapoints:
                    dtypes = dict([(x, 'str') for x in pkey])
                    for tc in time_concepts:
                        dtypes[tc] = int  # TODO: maybe there are other time format?
                    df = dd.read_csv(os.path.join(base_dir, r['path']), dtype=dtypes)
                    try:
                        indicator_name = list(set(df.columns) - set(pkey))[0]
                    except:
                        print(df.columns)
                        print(pkey)
                        raise
                    keys = tuple(sorted(pkey))
                    if indicator_name in datapoints.keys():
                        if keys in datapoints[indicator_name]:
                            datapoints[indicator_name][keys].append(df)
                        else:
                            datapoints[indicator_name][keys] = [df]
                    else:
                        datapoints[indicator_name] = dict()
                        datapoints[indicator_name][keys] = [df]

        # datapoints
        if not no_datapoints:
            for i, v in datapoints.items():
                for k, l in v.items():
                    v[k] = dd.multi.concat(l)

        # entities
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
            df = l[0]
            for df_ in l[1:]:
                df = pd.merge(df, df_, on=e, how='outer')
                for c in list(df.columns):
                    if c.endswith('_x'):
                        c_orig = c[:-2]
                        df[c_orig] = None
                        for i in df.index:
                            if not pd.isnull(df.loc[i, c]):
                                if not pd.isnull(df.loc[i, c_orig+'_y']):
                                    assert df.loc[i, c] == df.loc[i, c_orig+'_y'], \
                                        "different values for same cell:{}, {}".format(i, c)
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
        ddf_schema = {'concepts': [], 'entities': [], 'datapoints': []}
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
        dtypes = dict([(c, 'str') for c in all_entity_concepts])  # set all entity column to string type

        def _which_sets(entity_, domain_):
            try:
                return entity_value_cache[domain_][entity_]
            except KeyError:
                logging.debug('entity {} is not in {} domain!'.format(entity_, domain_))
                raise

        def _gen_key_value_object(resource):
            logging.debug('working on: {}'.format(resource['path']))
            base_dir = self.base_dir
            data = pd.read_csv(os.path.join(base_dir, resource['path']), dtype=dtypes)
            if isinstance(resource['schema']['primaryKey'], str):
                pkeys = [resource['schema']['primaryKey']]
            else:
                pkeys = resource['schema']['primaryKey']

            entity_cols = [x for x in pkeys if
                           (x in cdf.index) and
                           (cdf.loc[x, 'concept_type'] in ['entity_set', 'entity_domain'])]
            value_cols = list(set([x['name'] for x in resource['schema']['fields']]) - set(pkeys))
            # only consider all permutations on entity columns
            if len(entity_cols) > 0:
                data = data[pkeys].drop_duplicates(subset=entity_cols)

            all_permutations = set()
            for i, r in data.iterrows():
                perm = list()
                for c in pkeys:
                    if c not in cdf.index:
                        perm.append(tuple([c]))
                        continue
                    if cdf.loc[c, 'concept_type'] == 'entity_set':
                        domain = cdf.loc[c, 'domain']
                        perm.append(_which_sets(r[c], domain))
                    elif cdf.loc[c, 'concept_type'] == 'entity_domain':
                        perm.append(_which_sets(r[c], c))
                    else:
                        perm.append(tuple([c]))
                all_permutations.add(tuple(perm))

            for row in all_permutations:
                for perm in product(*row):
                    if len(value_cols) > 0:
                        for c in value_cols:
                            yield {'primaryKey': list(perm), 'value': c, 'resource': resource['name']}
                    else:
                        yield {'primaryKey': list(perm), 'value': None, 'resource': resource['name']}

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

        pbar = tqdm(total=len(self.resources))
        for g in map(_gen_key_value_object, self.resources):
            for kvo in g:
                # logging.debug("adding kvo {}".format(str(kvo)))
                _add_to_schema(kvo)
            pbar.update(1)

        for sch in hash_table.values():
            sch['resources'] = list(sch['resources'])
            if len(sch['primaryKey']) == 1:
                if sch['primaryKey'][0] == 'concept':
                    ddf_schema['concepts'].append(sch)
                else:
                    ddf_schema['entities'].append(sch)
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
