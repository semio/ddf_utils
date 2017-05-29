# -*- coding: utf-8 -*-

"""datapackage model"""

import os
import os.path as osp
import pandas as pd
from .ddf import Dataset
from itertools import product
from .utils import load_datapackage_json

import logging


class Datapackage:
    def __init__(self, datapackage_path):
        try:
            self.base_dir, self.datapackage = load_datapackage_json(datapackage_path)
        except FileNotFoundError:
            logging.warning("no datapackage.json found")
            raise

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

    def load(self, **kwargs):
        return Dataset.from_ddfcsv(self.base_dir, **kwargs)

    def generate_ddfschema(self):
        ds = self.load(no_datapoints=True)
        cdf = ds.concepts.set_index('concept')
        hash_table = {}
        ddf_schema = {'concepts': [], 'entities': [], 'datapoints': []}

        def _which_sets(entity, domain):
            ent_df = ds.get_entity(domain).set_index(domain)
            sets = [domain]
            for c in ent_df.columns:
                if c.startswith('is--'):
                    if ent_df.loc[entity, c] is True:
                        sets.append(c[4:])
            return sets

        def _gen_key_value_object(resource):
            base_dir = self.base_dir
            data = pd.read_csv(os.path.join(base_dir, resource['path']))
            if isinstance(resource['schema']['primaryKey'], str):
                pkeys = [resource['schema']['primaryKey']]
            else:
                pkeys = resource['schema']['primaryKey']

            value_cols = list(set([x['name'] for x in resource['schema']['fields']]) - set(pkeys))

            pkeys_dict = dict()

            for k in pkeys:
                if k in cdf.index:
                    if cdf.loc[k, 'concept_type'] == 'entity_set':
                        domain = cdf.loc[k, 'domain']
                        for val in data[k].unique():
                            if k in pkeys_dict.keys():
                                pkeys_dict[k] = list(set(_which_sets(val, domain)).union(set(pkeys_dict[k])))
                            else:
                                pkeys_dict[k] = _which_sets(val, domain)
                    elif cdf.loc[k, 'concept_type'] == 'entity_domain':
                        domain = k
                        for val in data[k].unique():
                            if k in pkeys_dict.keys():
                                pkeys_dict[k] = list(set(_which_sets(val, domain)).union(set(pkeys_dict[k])))
                            else:
                                pkeys_dict[k] = _which_sets(val, domain)
                    else:
                        pkeys_dict[k] = [k]
                else:
                    pkeys_dict[k] = [k]

            for perm in product(*(list(pkeys_dict.values()))):
                for c in value_cols:
                    yield {'primaryKey': list(perm), 'value': c, 'resource': resource['name']}

        def _add_to_schema(resource_schema):
            key = '-'.join(sorted(resource_schema['primaryKey']))
            hash_val = key + '--' + resource_schema['value']
            if hash_val not in hash_table.keys():
                hash_table[hash_val] = {
                    'primaryKey': sorted(resource_schema['primaryKey']),
                    'value': resource_schema['value'],
                    'resources': [resource_schema['resource']]
                }
            else:
                hash_table[hash_val]['resources'].append(resource_schema['resource'])

        for g in map(_gen_key_value_object, self.resources):
            for kvo in g:
                _add_to_schema(kvo)

        for sch in hash_table.values():
            if len(sch['primaryKey']) == 1:
                if sch['primaryKey'][0] == 'concept':
                    ddf_schema['concepts'].append(sch)
                else:
                    ddf_schema['entities'].append(sch)
            else:
                ddf_schema['datapoints'].append(sch)

        return ddf_schema

