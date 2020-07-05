# -*- coding: utf-8 -*-

"""datapackage model"""

import os.path as osp
from typing import List, Tuple, Dict, Union, Callable
import attr
import json
from itertools import product
from collections import OrderedDict
from tqdm import tqdm

import pandas as pd

from .ddf import DDF, Concept, EntityDomain, Entity, DaskDataPoint, Synonym
from .utils import absolute_path

import logging

logger = logging.getLogger(__name__)


@attr.s(auto_attribs=True, repr=False)
class TableSchema:
    """Table Schema Object Class"""
    fields: List[dict]
    primaryKey: Union[List[str], str]

    @classmethod
    def from_dict(cls, d: dict):
        fields = d['fields']
        primaryKey = d['primaryKey']
        return cls(fields, primaryKey)

    @property
    def field_names(self):
        return [f['name'] for f in self.fields]

    @property
    def common_fields(self):
        field_names = self.field_names
        pkey = self.primaryKey
        if isinstance(pkey, str):
            common_fields = list(filter(lambda x: x != pkey, field_names))
        else:
            common_fields = list(filter(lambda x: x not in pkey, field_names))
        return common_fields

    def __repr__(self):
        return "TableSchema(primaryKey: {}, fields: {})".format(self.primaryKey, self.common_fields)


@attr.s(auto_attribs=True)
class Resource:
    name: str
    path: str
    schema: TableSchema

    @classmethod
    def from_dict(cls, d: dict):
        path = d['path']
        name = d['name']
        schema = TableSchema.from_dict(d['schema'])
        return cls(name, path, schema)

    def to_dict(self):
        res = vars(self).copy()
        if 'schema' in res:
            res['schema'] = vars(res['schema']).copy()
        return res


@attr.s(auto_attribs=True)
class DDFSchema:
    primaryKey: List[str]
    value: str
    resources: List[str]  # a list of resource names

    @classmethod
    def from_dict(cls, d: dict):
        primaryKey = d['primaryKey']
        value = d['value']
        resources = d['resources']
        return cls(primaryKey=primaryKey, value=value, resources=resources)


@attr.s(auto_attribs=True, repr=False)
class DataPackage:
    base_path: str
    resources: List[Resource]
    props: dict = attr.ib(factory=dict)

    def __attrs_post_init__(self):
        self.base_path = absolute_path(self.base_path)

    def __repr__(self):
        return f"DataPackage({self.base_path})"

    @classmethod
    def from_dict(cls, d_: dict, base_path='./'):
        d = d_.copy()
        resources = list(map(Resource.from_dict, d.pop('resources')))
        return cls(base_path=base_path, resources=resources, props=d)

    @classmethod
    def from_json(cls, json_path):
        json_path = absolute_path(json_path)
        base_path = osp.dirname(json_path)
        d = json.load(open(json_path))
        return cls.from_dict(d, base_path)

    @classmethod
    def from_path(cls, path):
        path = absolute_path(path)
        json_path = osp.join(path, 'datapackage.json')
        return cls.from_json(json_path)

    def to_dict(self):
        """dump the datapackage to disk"""
        raise NotImplementedError


@attr.s(repr=False)
class DDFcsv(DataPackage):
    """DDFCSV datapackage."""
    ddfSchema: Dict[str, List[DDFSchema]] = attr.ib(factory=dict)
    ddf: DDF = attr.ib(init=False)
    concepts_resources: List[Resource] = attr.ib(init=False)
    entities_resources: List[Resource] = attr.ib(init=False)
    datapoints_resources: List[Resource] = attr.ib(init=False)
    synonyms_resources: List[Resource] = attr.ib(init=False)

    # config for read_csv
    _default_reader_options = {'keep_default_na': False, 'na_values': ['']}

    def __attrs_post_init__(self):
        super(DDFcsv, self).__attrs_post_init__()
        conc = list()
        ent = list()
        dp = list()
        syn = list()
        for r in self.resources:
            pkey = r.schema.primaryKey
            if isinstance(pkey, str):
                if pkey == 'concept':
                    conc.append(r)
                else:
                    ent.append(r)
            else:  # TODO: datapoints key might be one column, not list of columns?
                if 'synonym' in pkey:
                    syn.append(r)
                else:
                    dp.append(r)
        self.concepts_resources = conc
        self.entities_resources = ent
        self.datapoints_resources = dp
        self.synonyms_resources = syn
        self.ddf = self.load_ddf()

    @classmethod
    def from_dict(cls, d_: dict, base_path='./'):
        d = d_.copy()
        resources = list(map(Resource.from_dict, d.pop('resources')))
        if 'ddfSchema' in d.keys():
            ddf_schema_ = d.pop('ddfSchema')
            ddf_schema = dict()
            for k, v in ddf_schema_.items():
                ddf_schema[k] = [DDFSchema.from_dict(d) for d in v]
        else:
            ddf_schema = {}
        return cls(base_path=base_path, resources=resources, ddfSchema=ddf_schema, props=d)

    def to_dict(self):
        res = OrderedDict(self.props.copy())
        res['resources'] = [r.to_dict() for r in self.resources]
        if self.ddfSchema:
            res['ddfSchema'] = dict()
            for k, v in self.ddfSchema.items():
                res['ddfSchema'][k] = [vars(sch).copy() for sch in v]
        return res

    def _gen_concepts(self):
        concepts_paths = [osp.join(self.base_path, r.path) for r in self.concepts_resources]
        for p in concepts_paths:
            df = pd.read_csv(p, index_col='concept', dtype=str, **self._default_reader_options)
            for concept, row in df.iterrows():
                concept_type = row['concept_type']
                props = row.drop('concept_type').to_dict()
                yield (concept, Concept(id=concept, concept_type=concept_type, props=props))

    def _gen_entities(self, concepts: Dict[str, Concept]):
        for r in self.entities_resources:
            pkey = r.schema.primaryKey
            if concepts[pkey].concept_type == 'entity_domain':
                domain = concepts[pkey].id
            else:
                domain = concepts[pkey].props['domain']

            df = pd.read_csv(osp.join(self.base_path, r.path), dtype=str,  # TODO: is it okay to use str for all?
                             **self._default_reader_options)
            df = df.set_index(pkey)
            is_cols = list(filter(lambda x: x.startswith('is--'), df.columns.values))
            for ent, row in df.iterrows():
                sets = list()
                for c in is_cols:
                    if row[c] == 'TRUE' and c[4:] != domain:
                        sets.append(c[4:])  # strip the 'is--' part, only keep set name
                yield (domain, Entity(id=ent, domain=domain, sets=sets, props=row.drop(is_cols).to_dict()))

    def _gen_datapoints(self):
        for r in self.datapoints_resources:
            fields = r.schema.common_fields
            pkey = r.schema.primaryKey
            for f in fields:
                yield (f, pkey, osp.join(self.base_path, r.path))

    def _gen_synonyms(self):
        for r in self.synonyms_resources:
            # there should be only two columns
            pkey = r.schema.primaryKey
            if pkey[0] == 'synonym':
                concept = pkey[1]
            else:
                concept = pkey[0]
            df = pd.read_csv(osp.join(self.base_path, r.path), **self._default_reader_options)
            sym = Synonym(concept_id=concept, synonyms=df.set_index('synonym')[concept].to_dict())
            yield (concept, sym)

    @staticmethod
    def entity_domain_to_categorical(domain: EntityDomain):
        entities = [e.id for e in domain.entities]
        return pd.api.types.CategoricalDtype(entities)

    @staticmethod
    def entity_set_to_categorical(domain: EntityDomain, s: str):
        entity_set = domain.get_entity_set(s)
        entities = [e.id for e in entity_set]
        return pd.api.types.CategoricalDtype(entities)

    def load_ddf(self):
        """-> DDF"""
        # load concepts
        concepts = dict(self._gen_concepts())

        # load entities
        entities = list(self._gen_entities(concepts))
        domains = dict()
        domains_tmp = dict()
        for domain, entity in entities:
            if domain not in domains_tmp.keys():
                domains_tmp[domain] = list()
            domains_tmp[domain].append(entity)

        for domain, entities_ in domains_tmp.items():
            # TODO: maybe get properties from concepts table
            # Allow duplicated entity because they may be defined in multiple resources
            # i.e. multiple entity sets in separated files.
            domains[domain] = EntityDomain.from_entity_list(domain_id=domain, entities=entities_, allow_duplicated=True)

        # load datapoints. Here we will use Dask for all
        # 1. create categories for entity domains
        dtypes = dict()
        # parse_dates = list()
        concept_types = dict()
        for domain_name, domain in domains.items():
            dtypes[domain_name] = self.entity_domain_to_categorical(domain)
            for eset in domain.entity_sets:
                dtypes[eset] = self.entity_set_to_categorical(domain, eset)
        # 2. get all concept types, update dtypes for time concepts
        for c_id, c in concepts.items():
            concept_types[c_id] = c.concept_type
            if c.concept_type == 'time':
                dtypes[c_id] = 'str'
        # 3. group files for same indicator together
        indicators = dict()
        for field, pkey, path in self._gen_datapoints():
            # import ipdb; ipdb.set_trace()
            indicator = field
            pkey = tuple(sorted(pkey))
            if indicator not in indicators:
                indicators.setdefault(indicator, dict([(pkey, [path])]))
            else:
                if pkey not in indicators[indicator]:
                    indicators[indicator][pkey] = [path]
                else:
                    indicators[indicator][pkey].append(path)
        datapoints = dict()
        for i, v in indicators.items():
            datapoints[i] = dict()
            dtypes_ = dtypes.copy()
            # dtypes_[i] = 'float'  # TODO: supporting string/float datatypes, not just float
            read_csv_options = dict(dtype=dtypes)
            for k, paths in v.items():
                dp = DaskDataPoint(id=i, dimensions=k, path=paths, concept_types=concept_types, read_csv_options=read_csv_options)
                datapoints[i][k] = dp

        # load synonyms
        synonyms = dict(self._gen_synonyms())

        # return complete DDF object
        return DDF(concepts=concepts, entities=domains, datapoints=datapoints, synonyms=synonyms, props=self.props)

    def generate_ddf_schema(self, progress_bar=False):
        """generate ddf schema from all resources.

        Parameters
        ----------

        progress_bar : bool
            whether progress bar should be shown when generating ddfSchema.

        """
        hash_table = {}
        ddf_schema = {'concepts': [], 'entities': [], 'datapoints': [], 'synonyms': []}
        entity_value_cache = dict()
        dtypes = dict()

        # check if we need progress bar
        if progress_bar:
            if logger.getEffectiveLevel() == 10:  # debug: force not showing progress bar
                logger.warning("progress bar will be disabled in debugging mode.")
                progress_bar = False

        # generate set-membership details for every single entity in dataset
        # also create dtypes for later use
        for domain_id, domain in self.ddf.entities.items():
            dtypes[domain_id] = self.entity_domain_to_categorical(domain)
            for s in self.ddf.entities[domain_id].entity_sets:
                dtypes[s] = self.entity_set_to_categorical(domain, s)
            entity_value_cache[domain_id] = dict()
            for ent in domain.entities:
                sets = set()
                sets.add(domain_id)
                for s in ent.sets:
                    sets.add(s)
                entity_value_cache[domain_id][ent.id] = tuple(sets)

        def _which_sets(entity_, domain_):
            try:
                return entity_value_cache[domain_][entity_]
            except KeyError:
                logger.debug('entity {} is not in {} domain!'.format(entity_, domain_))
                raise

        def _gen_key_value_object(resource: Resource):
            logger.debug('working on: {}'.format(resource.path))
            if isinstance(resource.schema.primaryKey, str):
                pkeys = [resource.schema.primaryKey]
            else:
                pkeys = resource.schema.primaryKey

            entity_cols = [x for x in pkeys
                           if x in self.ddf.concepts
                           and self.ddf.concepts[x].concept_type in ['entity_domain', 'entity_set']]
            value_cols = resource.schema.common_fields
            data = pd.read_csv(osp.join(self.base_path, resource.path), dtype=dtypes,
                               **self._default_reader_options)
            # check if entity columns data match entity defined in entity files
            for c in entity_cols:
                if data[c].hasnans:
                    data_ = pd.read_csv(osp.join(self.base_path, resource.path), dtype={c: str}, **self._default_reader_options)
                    ents = dtypes[c].categories.values
                    ents_ = data_[c].unique()
                    diff = set(ents_) - set(ents)
                    logger.critical("in file {}:".format(resource.path))
                    logger.critical("{} column contains entity which does not belong to {} domain/set: {}".format(c, c, list(diff)))
                    raise ValueError("entity mismatch")

            # for resources that have entity_columns: only consider all permutations on entity columns
            if len(entity_cols) > 0:
                data = data[entity_cols].drop_duplicates()

            pkeys_prop = dict()
            for c in pkeys:
                if c == 'cocnept':
                    pkeys_prop[c] = {'type': 'concept'}
                elif c not in self.ddf.concepts:
                    pkeys_prop[c] = {'type': 'non_concept'}
                else:
                    concept = self.ddf.concepts[c]
                    if concept.concept_type == 'entity_set':
                        pkeys_prop[c] = {'type': 'entity_set',
                                         'domain': concept.props['domain']}
                    elif concept.concept_type == 'entity_domain':
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

            # if data is empty. Just emit an object with primarykey and null value
            if len(all_permutations) == 0:
                obj = {'primaryKey': pkeys, 'value': None, 'resource': resource.name}
                logger.debug('yielding: {}'.format(str(obj)))
                yield obj

            for row in all_permutations:
                for perm in product(*row):
                    if len(value_cols) > 0:
                        for c in value_cols:
                            obj = {'primaryKey': list(perm), 'value': c, 'resource': resource.name}
                            logger.debug('yielding: {}'.format(str(obj)))
                            yield obj
                    else:
                        obj = {'primaryKey': list(perm), 'value': None, 'resource': resource.name}
                        logger.debug('yielding: {}'.format(str(obj)))
                        yield obj

        def _add_to_schema(resource_schema):
            """handle objects generated by ``_gen_key_value_object``"""
            key = '-'.join(sorted(resource_schema['primaryKey']))
            if not pd.isnull(resource_schema['value']):
                hash_val = key + '--' + resource_schema['value']
            else:
                hash_val = key + '--' + 'nan'
            if hash_val not in hash_table.keys():
                hash_table[hash_val] = {
                    'primaryKey': sorted(resource_schema['primaryKey']),
                    'value': resource_schema['value'],
                    'resources': {resource_schema['resource']}
                }
            else:
                hash_table[hash_val]['resources'].add(resource_schema['resource'])

        # make progressbar and run the process to generate schema
        if progress_bar:
            pbar = tqdm(total=len(self.resources))

        for g in map(_gen_key_value_object, self.resources):
            if progress_bar:
                pbar.update(1)
            for kvo in g:
                logging.debug("adding kvo {}".format(str(kvo)))
                _add_to_schema(kvo)

        if progress_bar:
            pbar.close()

        for sch in hash_table.values():
            sch['resources'] = list(sch['resources'])  # convert set to list
            sch_object = DDFSchema.from_dict(sch)
            if len(sch['primaryKey']) == 1:
                if sch['primaryKey'][0] == 'concept':
                    ddf_schema['concepts'].append(sch_object)
                else:
                    ddf_schema['entities'].append(sch_object)
            else:
                if 'synonym' in sch['primaryKey']:
                    ddf_schema['synonyms'].append(sch_object)
                else:
                    ddf_schema['datapoints'].append(sch_object)

        return ddf_schema

    def get_ddf_schema(self, update=False):
        if not update and self.ddfSchema is not None:
            return self.ddfSchema
        elif not update and self.ddfSchema is None:
            raise ValueError('No ddfSchema, please use update=True to generate one')
        else:
            self.ddfSchema = self.generate_ddf_schema()
            return self.ddfSchema
