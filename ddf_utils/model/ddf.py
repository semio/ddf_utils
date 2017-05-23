# -*- coding: utf-8 -*-

"""The DDF model"""

import os
import os.path as osp
import json
import numpy as np
import pandas as pd
import dask.dataframe as dd
import xarray
import hashlib


def load_datapackage_json(path):
    if osp.isdir(path):
        dp = json.load(open(osp.join(path, 'datapackage.json')))
        basedir = path
    else:
        dp = json.load(open(path))
        basedir = osp.dirname(path)

    return basedir, dp


class Dataset():
    """DDF dataset"""
    def __init__(self, concepts=None, entities=None, datapoints=None, datapackage=None):
        """create a Dataset object

        concepts: dataframe with all concepts definition

        entities: dictionary of entity name and dataframe mapping

        datapoints: dictionary of indicator name and primarykey -> dataframe mapping.
        So it's a nested dictionary. Note that datapoints are dask dataframes.
        """
        # TODO: add type check.
        self._concepts = concepts
        self._entities = entities
        self._datapoints = datapoints
        self._datapackage = datapackage

    def __repr__(self):
        concs = self.concepts.set_index('concept')
        docs = ["<Dataset {}>".format(self.datapackage['name'])]
        docs.append("entities:")
        if 'entity_set' in concs['concept_type'].values:
            for domain, entities in self.entities.items():
                if len(concs[concs.domain == domain]) == 0:
                    vals = self.get_entity(domain)[domain].head().values
                    docs.append('\t{}:\t\t{}...'.format(domain, ', '.join(map(str, vals))))
                else:
                    docs.append('\t{}:'.format(domain))
                    sets = concs[concs.domain == domain]
                    for i in sets.index:
                        vals = self.get_entity(i)[domain].head().values
                        string = ', '.join(map(str, vals))
                        docs.append('\t\t{}:\t\t{},...'.format(i, string))
        else:
            for domain, entities in self.entities.items():
                vals = self.get_entity(domain)[domain].head().values
                docs.append('\t{}:\t\t{},...'.format(domain, ','.join(map(str, vals))))

        docs.append('indicators:')
        for i, data in self.datapoints.items():
            docs.append('\t{}, by:'.format(i))
            for keys in data.keys():
                docs.append('\t\t{}'.format(keys))

        return '\n'.join(docs)

    @classmethod
    def from_ddfcsv(cls, datapackage):
        """read from local DDF csv dataset.

        datapackage: path to the datapackage folder or datapackage.json
        """
        concepts = list()
        entities_ = list()  # temp, the final data will be entities
        entities = dict()
        datapoints = dict()

        base_dir, dp = load_datapackage_json(datapackage)

        for r in dp['resources']:
            pkey = r['schema']['primaryKey']
            if pkey == 'concept':
                concepts.append(pd.read_csv(osp.join(base_dir, r['path'])))
            elif isinstance(pkey, str):
                entities_.append(
                    {
                        "data": pd.read_csv(osp.join(base_dir, r['path'])),
                        "key": pkey
                    })
            else:
                df = dd.read_csv(os.path.join(base_dir, r['path']))
                indicator_name = list(set(df.columns) - set(pkey))[0]
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
        for i, v in datapoints.items():
            for k, l in v.items():
                v[k] = dd.multi.concat(l)

        # concepts
        concepts = pd.concat(concepts)

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
            entities[e] = pd.concat(l, ignore_index=True)

        return cls(concepts=concepts, entities=entities,
                   datapoints=datapoints, datapackage=dp)

    @property
    def concepts(self):
        return self._concepts

    @property
    def indicators(self):
        return list(self._datapoints.keys())

    @property
    def datapoints(self):
        return self._datapoints

    @property
    def datapackage(self):
        return self._datapackage

    @property
    def entities(self):
        return self._entities

    def get_entity(self, ent):
        conc = self.concepts.set_index('concept')
        if conc.loc[ent, 'concept_type'] == 'entity_domain':
            return self.entities[ent]
        else:
            ents = self.entities
            domain = conc.loc[ent, 'domain']
            ent_domain = ents[domain]
            return ent_domain[ent_domain['is--'+ent] == True].dropna(axis=1, how='all')

    def get_datapoint_df(self, indicator, primary_key=None):
        if primary_key:
            return self.datapoints[indicator][tuple(sorted(list(primary_key)))]
        else:
            return list(self.datapoints[indicator].items())

    def validate(self, **options):
        """validate the dataset"""
        raise NotImplementedError

    def _update_inplace(self, ds):
        self._concepts = ds.concepts
        self._entities = ds.entities
        self._datapoints = ds.datapoints
        self._datapackage = ds.datapackage

    def update_datapackage(self, keep_meta=True):
        """update datapackage with new resources"""
        raise NotImplementedError

    def to_ddfcsv(self, **options):
        """save data to disk"""
        raise NotImplementedError