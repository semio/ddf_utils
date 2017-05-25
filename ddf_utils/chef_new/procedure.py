# -*- coding: utf-8 -*-

"""procedures, base on dataset"""

import logging
from .helpers import accept_file_input
from ..model.ddf import Dataset

logging.getLogger('Chef')


@accept_file_input(['dictionary'])
def translate_concept(chef, dataset=None, dictionary=None) -> Dataset:
    """translate concepts, base on dictionary"""
    ds = chef.get_ingredient(dataset)
    return ds.rename(concepts=dictionary)


def _gen_mapping_dict(df, column, dictionary):
    pass


def translate_entity(dataset: Dataset, dictionary, column, *,
                     target_column=None, not_found='drop',
                     ambiguity='prompt', ignore_case=False):
    """translate entity values base on dictionary"""
    if sorted(dictionary.keys()) == sorted(['base', 'key', 'value']):
        dictionary = _gen_mapping_dict(dataset.get_entity(target_column), target_column, dictionary)

    raise NotImplementedError


def split_entity(dataset, dictionary, target_entity, splitted='drop'):
    """split entity to new entities"""

    from ..transformer import split_keys

    concepts, entities, datapoints = dataset.get_data_copy()
    new_entities = set()

    # update datapoints
    for indicator, kvs in datapoints.items():
        for k, df in kvs.items():
            if target_entity in k:
                kvs[k] = split_keys(df.set_index(k),
                                    target_entity, dictionary, splitted).reset_index()
                new_entities = new_entities.union(set(kvs[k][target_entity].unique()))

    # update entities
    if target_entity in dataset.domains:
        pass


def merge_entity(dataset):
    pass


def trend_bridge(dataset):
    pass


def run_op(dataset):
    pass


def window(dataset):
    pass


def groupby(dataset):
    pass


def filter_row(dataset):
    pass


def merge(dataset):
    pass


# functions for add datapoints/concepts/entities
#