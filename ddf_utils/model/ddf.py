# -*- coding: utf-8 -*-

"""The DDF model"""

import os.path as osp
from typing import List, Tuple, Dict, Union, Callable
from abc import ABC, abstractmethod
import attr
import json
from pathlib import Path
from itertools import product
from tqdm import tqdm
from collections import OrderedDict, Counter

import numpy as np
import pandas as pd
import dask.dataframe as dd

from ddf_utils.str import parse_time_series

import logging

logger = logging.getLogger(__name__)


@attr.s(auto_attribs=True)
class Concept:
    id: str
    concept_type: str
    props: dict = attr.ib(factory=dict)

    def to_dict(self):
        res = OrderedDict()
        res['concept'] = self.id
        res['concept_type'] = self.concept_type
        props = self.props.copy()
        for k, v in props.items():
            res[k] = v
        return res


@attr.s(auto_attribs=True)
class Entity:
    id: str
    domain: str
    sets: List[str]
    props: dict = attr.ib(factory=dict)

    def to_dict(self, pkey=None):
        """create a dictionary containing name/domain/is--headers/and properties
        So this can be easily plug in pandas.DataFrame.from_records()
        """
        res = OrderedDict()
        if pkey:
            res[pkey] = self.id
        else:
            res[self.domain] = self.id
        if self.sets:
            for s in self.sets:
                header = f'is--{s}'
                res[header] = 'TRUE'
        props = self.props.copy()
        for k, v in props.items():
            res[k] = v

        return res


# @attr.s(auto_attribs=True)
# class EntitySet:
#     id: str
#     entities: List[Entity]
#     domain: str
#     props: dict = attr.ib(factory=dict)


@attr.s(auto_attribs=True)
class EntityDomain:
    id: str
    entities: List[Entity] = attr.ib(factory=list)
    props: dict = attr.ib(factory=dict)

    @entities.validator
    def _check_entities_identity(self, attribute, value):
        """double check entities provided in initialization

        1. domains should be same as the domain id
        2. an entity should be exists only once in the list
        """
        if len(value) == 0:  # do nothing if the entities list is empty.
            return
        entities_id_list = [x.id for x in self.entities]
        entities_domain_set = set([x.domain for x in self.entities])
        if len(entities_domain_set) > 1 or self.id not in entities_domain_set:
            other_domains = list(entities_domain_set - set([self.id]))
            raise ValueError(f"there are entities with different domain: {other_domains}. Expected domain: {self.id}")
        counter = Counter(entities_id_list)
        error = False
        for k, v in counter.items():
            if v > 1:
                logger.critical(f"entity {k} exists {v} times in entity table!")
                error = True
        if error:
            raise ValueError("duplicated entity detected")

    @classmethod
    def from_entity_list(cls, domain_id, entities, allow_duplicated=True, **kwargs):
        if not allow_duplicated:
            return cls(id=domain_id, entities=entities, props=kwargs)
        # if there are duplicates, we need to combine all duplicates
        # now construct a new entities list without duplicates
        entity_ids = [x.id for x in entities]
        entities_new = dict((i, Entity(id=i, domain=domain_id, sets=[], props={})) for i in entity_ids)
        for x in entities:
            en = entities_new[x.id]
            for s in x.sets:
                if s not in en.sets:
                    en.sets.append(s)
            en.props.update(x.props)
            entities_new[x.id] = en
        return cls(id=domain_id, entities=list(entities_new.values()), props=kwargs)

    @property
    def entity_ids(self):
        return [x.id for x in self.entities]

    @property
    def entity_sets(self):
        sets = set()
        for e in self.entities:
            for s in e.sets:
                sets.add(s)
        return list(sets)

    def get_entity_set(self, s):
        return [e for e in self.entities if s in e.sets]

    def has_entity(self, sid):
        return sid in self.entity_ids

    def to_dict(self, eset=None):
        if eset:
            entities = self.get_entity_set(eset)
            return [e.to_dict(eset) for e in entities]
        else:
            defaultdict = dict()
            for s in self.entity_sets:
                header = f'is--{s}'
                defaultdict[header] = 'FALSE'
            entities = self.entities
            res = list()
            for e in entities:
                d = e.to_dict()
                # appending False into the is--headers
                for k, v in defaultdict.items():
                    d.setdefault(k, v)
                res.append(d)
            return res

    def add_entity(self, ent: Entity):
        if ent.domain != self.id:
            logger.critical(f"trying to add Entity(id={ent.id}, domain={ent.domain}) to {self.id} domain")
            raise ValueError('domain name mismatch for the entity object and domain object!')
        for existing_ent in self.entities:
            if ent.id == existing_ent.id:
                logger.debug('updating existing entity: {}'.format(existing_ent.id))
                for s in ent.sets:
                    if s not in existing_ent.sets:
                        existing_ent.sets.append(s)
                # TODO: logging for existing fileds
                existing_ent.props.update(ent.props)
                break
        else:
            logger.debug('appending entity: {}'.format(ent.id))
            self.entities.append(ent)

    def __getitem__(self, item):
        return self.get_entity_set(item)


@attr.s(auto_attribs=True)
class DataPoint(ABC):
    """A DataPoint object stores a set of datapoints which have same dimensions and
    which belongs to only one indicator."""
    id: str
    dimensions: Tuple[str]
    store: str
    # TODO: think about cache

    @property
    @abstractmethod
    def data(self):
        ...


@attr.s
class PandasDataPoint(DataPoint):
    """load datapoints with pandas"""
    path: str = attr.ib()
    dtypes: dict = attr.ib()
    store = attr.ib(default='pandas')
    # data_cache = attr.ib()

    @property
    def data(self):
        cols = [*self.dimensions, self.id]
        return pd.read_csv(self.path, dtype=self.dtypes)[cols]


@attr.s
class DaskDataPoint(DataPoint):
    """load datapoints with dask"""
    path: Union[List[str], str] = attr.ib()  # can be a list of paths
    concept_types: dict = attr.ib()
    read_csv_options: dict = attr.ib(factory=dict)
    store = attr.ib(default='dask')

    @property
    def data(self):
        cols = [*self.dimensions, self.id]
        df = dd.read_csv(self.path, usecols=cols, **self.read_csv_options)
        # handling time columns
        # Because df.query() performance is poor when the df contains pd.Period dtype
        # we decided not to parse them, which will result in string dtype
        # TODO: use Period when perofrmance become better
        # for k, v in self.concept_types.items():
        #     if v == 'time':
        #         df[k] = parse_time_series(df[k], engine='dask')
        return df


@attr.s(auto_attribs=True, repr=False)
class Synonym:
    concept_id: str
    synonyms: Dict[str, str]

    def __repr__(self):
        dict_str = self.synonyms.__str__()
        if len(dict_str) > 20:
            dict_str = dict_str[:20] + ' ... }'
        return "Synonym(concept_id={}, synonyms={})".format(self.concept_id, dict_str)

    def to_dict(self):
        return {self.concept_id: self.synonyms}

    def to_dataframe(self):
        df = pd.DataFrame.from_dict(self.synonyms, orient='index', columns=[self.concept_id])
        df.index.name = ['synonym']
        return df.reset_index()


@attr.s(auto_attribs=True, repr=False)
class DDF:
    # Here I use dictionaries for the data structure, just for performance
    # in fact they can be just lists, i.e. concepts is just a list of Concept objects.
    concepts: Dict[str, Concept] = attr.ib(factory=dict)
    entities: Dict[str, EntityDomain] = attr.ib(factory=dict)
    datapoints: Dict[str, Dict[str, DataPoint]] = attr.ib(factory=dict)
    synonyms: Dict[str, Synonym] = attr.ib(factory=dict)
    props: dict = attr.ib(factory=dict)

    def __repr__(self):
        # def maybe_truncate(obj, maxlen=20, fillspaces=False):
        #     if isinstance(obj, np.ndarray):
        #         s = ', '.join(map(str, obj))
        #         if len(s) > maxlen:
        #             s = ','.join(s[:(maxlen - 3)].split(',')[:-1])
        #             s = s + '...'
        #     else:
        #         s = str(obj)
        #         if len(s) > maxlen:
        #             s = s[:(maxlen - 3)] + '...'
        #     if len(s) < maxlen and fillspaces:
        #         diff = maxlen - len(s)
        #         s = s + ' ' * diff
        #     return s

        indent = 4

        # TODO: also report domains/datapoints defined in concepts but without data.
        docs = ["<Dataset {}>".format(self.props.get('name', 'NONAME'))]
        docs.append("entity domains:")
        for domain_id, domain in self.entities.items():
            if not domain.entity_sets:
                docs.append(f'{" " * indent}* {domain_id}')
            else:
                docs.append(f'{" " * indent}* {domain_id}:')
                for ent_set in domain.entity_sets:
                    docs.append(f'{" " * indent * 2}- {ent_set}')
        docs.append("datapoints:")
        for indicator_id, ind_dict in self.datapoints.items():
            for pkey, _ in ind_dict.items():
                pkey_str = ', '.join(pkey)
                docs.append(f'{" " * indent}- ({pkey_str}) {indicator_id}')

        return '\n'.join(docs)

    def indicators(self, by=None):
        # TODO: naming
        if not by:
            return list(self.datapoints.keys())
        else:
            res = list()
            for i, v in self.datapoints.items():
                if tuple(sorted(by)) in v.keys():
                    res.append(i)
            return res

    def get_datapoints(self, i, by=None):
        # TODO: query by entity domain should work too.
        # current only entity set can be queried.
        if by:
            by_ = tuple(sorted(by))
        else:
            if len(self.datapoints[i]) > 1:
                raise ValueError("there are multiple primary keys for this indicator, "
                                 "you should provide the primary key")
            by_ = list(self.datapoints[i].keys())[0]

        return self.datapoints[i][by_]

    def get_entities(self, domain, eset=None):
        if not eset:
            return self.entities[domain].entities
        else:
            return [e for e in self.entities[domain].entities if eset in e.sets]

    def get_synonyms(self, concept):
        """get synonym dictionary. return None if no synonyms for the concept."""
        c = self.concepts[concept]
        if c.concept_type != 'entity_set':
            return self.synonyms.get(concept, None)

        # get synonyms for entity_set
        if concept in self.synonyms:
            return self.synonyms[concept]
        else:
            domain = c.props['domain']
            if domain not in self.synonyms:
                return None
            entities = self.get_entities(domain, eset=concept)
            ent_ids = [ent.id for ent in entities]
            res = dict()
            for k, v in self.synonyms[domain].synonyms.items():
                if str(v) in ent_ids:
                    res[k] = v
            return Synonym(concept_id=concept, synonyms=res)

    # TODO: maybe add below methods to modify DDF objects.
    # def get_concept_ids(self, concept_type=None):
    #     if concept_type is not None:
    #         return [concept.id for concept in self.concepts if concept.concept_type == concept_type]
    #     return [concept.id for concept in self.concepts]
    #
    # def get_indicators(self):
    #     dps = [(dp.id, dp.dimensions) for dp in self.datapoints]
    #
    # def add_concept(self, c: Concept):
    #     self.concepts.append(c)
    #
    # def remove_concept(self, c_id: str):
    #     self.concepts = [concept for concept in self.concepts if concept.id != c_id]
    #
    # def add_entity_domain(self, e: EntityDomain):
    #     self.entities.append(e)
    #
    # def remove_entity_domain(self, e_id: str):
    #     pass
    #
    # def add_datapoints(self, dps: DataPoint):
    #     self.datapoints.append(dps)
    #
    # def remove_datapoints(self, dps_id: str):
    #     pass

    # def validate(self):  # should be a function outside the class.
    #     """check if the DDF object is valid.
    #
    #     1. datapoints's dimensions should be time or entity domains/sets
    #     2. datapoints concepts and entity concepts should exists in concepts
    #     3. call every object's validate method.
    #     """
    #     pass
    #
    # def create_concepts(self):
    #     """create concepts from existing entity_domain/entity_set/datapoints"""
    #     pass
