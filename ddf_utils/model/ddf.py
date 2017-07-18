# -*- coding: utf-8 -*-

"""The DDF model"""

import os
import os.path as osp
from copy import deepcopy
import numpy as np
import pandas as pd
import dask.dataframe as dd
from .utils import load_datapackage_json


class Dataset:
    """DDF dataset"""
    def __init__(self, concepts=None, entities=None, datapoints=None, attrs=None):
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
        self.attrs = attrs

    def __repr__(self):

        def maybe_truncate(obj, maxlen=20, fillspaces=False):
            if isinstance(obj, np.ndarray):
                s = ', '.join(map(str, obj))
                if len(s) > maxlen:
                    s = ','.join(s[:(maxlen - 3)].split(',')[:-1])
                    s = s + '...'
            else:
                s = str(obj)
                if len(s) > maxlen:
                    s = s[:(maxlen - 3)] + '...'
            if len(s) < maxlen and fillspaces:
                diff = maxlen - len(s)
                s = s + ' ' * diff
            return s

        indent = 4

        concs = self.concepts.set_index('concept')
        docs = ["<Dataset {}>".format(self.attrs['name'])]
        docs.append("entities:")
        if 'entity_set' in concs['concept_type'].values:
            for domain, entities in self.entities.items():
                if len(concs[concs.domain == domain]) == 0:
                    vals = self.get_entity(domain)[domain].head(20).values
                    docs.append('{}- {}{}{}'.format(' ' * indent,
                                                    maybe_truncate(domain, 10, True),
                                                    ' ' * indent * 2,
                                                    maybe_truncate(vals, 50)))
                else:
                    docs.append('{}{}:'.format(' ' * indent, domain))
                    sets = concs[concs.domain == domain]
                    for i in sets.index:
                        vals = self.get_entity(i)[i].head(20).values
                        docs.append('{}- {}{}{}'.format(' ' * indent * 2,
                                                        maybe_truncate(i, 10, True),
                                                        ' ' * indent * 2,
                                                        maybe_truncate(vals, 50)))
        else:
            for domain, entities in self.entities.items():
                vals = self.get_entity(domain)[domain].head(20).values
                docs.append('{}- {}{}{}'.format(' ' * indent,
                                                maybe_truncate(domain, 10, True),
                                                ' ' * indent * 2,
                                                maybe_truncate(vals, 50)))

        docs.append('indicators:')
        for i, data in self.datapoints.items():
            docs.append('{}{}, by:'.format(' ' * indent, maybe_truncate(i)))
            for keys in data.keys():
                docs.append('{}{}'.format(' ' * indent * 2, keys))

        return '\n'.join(docs)

    @property
    def concepts(self):
        return self._concepts

    @property
    def datapoints(self):
        return self._datapoints

    @property
    def entities(self):
        return self._entities

    @property
    def domains(self):
        return list(self.entities.keys())

    @property
    def is_empty(self):
        if self.concepts is None and self.entities is None and self.datapoints is None:
            return True
        else:
            return False

    def indicators(self, by=None):
        if not by:
            return list(self._datapoints.keys())

        res = list()
        by = set(by)
        for i, kvs in self._datapoints.items():
            for k, v in kvs.items():
                if by == set(k):
                    res.append(i)
        return res

    def get_entity(self, ent):
        conc = self.concepts.set_index('concept')
        if conc.loc[ent, 'concept_type'] == 'entity_domain':
            return self.entities[ent]
        else:
            domain = conc.loc[ent, 'domain']
            ent_domain = self.entities[domain]
            return (ent_domain[ent_domain['is--'+ent] == 'TRUE']
                    .dropna(axis=1, how='all')
                    .rename(columns={domain: ent}))

    def get_datapoint_df(self, indicator, primary_key=None):
        if primary_key:
            return self.datapoints[indicator][tuple(sorted(list(primary_key)))]
        else:
            return list(self.datapoints[indicator].items())

    def validate(self, **options):
        """validate the dataset"""
        raise NotImplementedError

    def get_data_copy(dataset):
        concepts = dataset.concepts.copy()
        entities = deepcopy(dataset.entities)
        datapoints = deepcopy(dataset.datapoints)

        return (concepts, entities, datapoints)

    def _update_inplace(self, ds):
        self._concepts = ds.concepts
        self._entities = ds.entities
        self._datapoints = ds.datapoints
        self.attrs = ds.attrs

    def _rename_concepts(self, dictionary, inplace=False):
        concepts, entities, datapoints = self.get_data_copy()

        if self.concepts is not None:
            concepts = concepts.rename(columns=dictionary)
            concepts.concept = concepts.concept.map(lambda x: dictionary[x] if x in dictionary.keys() else x)

        # translate entities
        if self.entities is not None:
            keys_orig = list(entities.keys())

            for e, df in entities.items():
                entities[e] = df.rename(columns=dictionary)
            for k, v in dictionary.items():  # also change the keys in entities dict
                if k in keys_orig:
                    df = entities.pop(k)
                    entities[v] = df

        # translate datapoints
        if self.datapoints is not None:
            indicators_orig = list(datapoints.keys())

            for i in indicators_orig:
                keys_orig = list(datapoints[i].keys())
                for keys in keys_orig:
                    datapoints[i][keys] = datapoints[i][keys].rename(columns=dictionary)

                    ks = list(keys)
                    ks_ = [dictionary[x]
                           if x in dictionary.keys()
                           else x
                           for x in ks]
                    if not ks == ks_:
                        ks_ = tuple(sorted(ks_))
                        df = datapoints[i].pop(keys)
                        datapoints[i][ks_] = df
                if i in dictionary.keys():
                    di = datapoints.pop(i)
                    datapoints[dictionary[i]] = di

        res = Dataset(concepts=concepts, entities=entities, datapoints=datapoints, attrs=self.attrs)
        if inplace:
            self._update_inplace(res)
        else:
            return res

    def _rename_entities(self, dictionary, inplace=False):
        raise NotImplementedError

    def rename(self, concepts=None, entities=None):
        """rename concepts or entities"""
        raise NotImplementedError

    def to_ddfcsv(self, out_dir, **kwargs):
        """save data to disk"""
        # concepts
        self.concepts.to_csv(osp.join(out_dir, 'ddf--concepts.csv'), index=False)

        # entities
        for domain, df in self.entities.items():
            fn = osp.join(out_dir, 'ddf--entities--{}.csv'.format(domain))

            # change lower case bool values in is--entity columns to upper case
            for c in df.columns:
                if c.startswith('is--'):
                    df[c] = df[c].map(lambda x: str(x).upper() if x else x)

            df.to_csv(fn, index=False)

        # datapoints. Because it's dask dataframe, we should compute it before save to disk
        for indicator, kvs in self.datapoints.items():
            for keys, df in kvs.items():
                keys_str = '--'.join(keys)
                fn = osp.join(out_dir, 'ddf--datapoints--{}--by--{}.csv'.format(indicator, keys_str))
                df.compute().to_csv(fn, index=False)
