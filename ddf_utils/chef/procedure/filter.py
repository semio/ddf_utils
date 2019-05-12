# -*- coding: utf-8 -*-

"""filter procedure for recipes"""


import logging

from typing import List, Sequence
from .. exceptions import ProcedureError
from .. helpers import debuggable, read_opt, query
from .. model.ingredient import Ingredient, get_ingredient_class
from .. model.chef import Chef


logger = logging.getLogger('filter')


def _check_item_type(item):
    """item filter only accept certain format"""
    if isinstance(item, Sequence):
        return "list"
    elif isinstance(item, dict) and len(item) == 1:
        assert list(item.keys())[0] in ['$in', '$nin'], "only $in, $nin supported in item filter"
        return "mongo"
    else:
        raise ProcedureError("not supported item filter: " + str(item))


def _filter_datapoint_item(ingredient, item):

    data = ingredient.get_data()

    def filter_in_list(d, l):
        r = dict()
        for i in l:
            if i in d.keys():
                r[i] = d[i].copy()
            else:
                logger.warning("concept {} not found in ingredient {}".format(i, ingredient.id))
        return r

    item_type = _check_item_type(item)
    if item_type == 'list':
        res = filter_in_list(data, item)
    else:  # mongo
        selector = list(item.keys())[0]
        item_list = list(item.values())[0]
        if selector == '$in':
            res = filter_in_list(data, item_list)
        else:  # $nin
            res = dict()
            for k, df in data.items():
                if k not in item_list:
                    res[k] = data[k].copy()
    return res


def _filter_other_item(ingredient, item):
    data = ingredient.get_data()
    item_type = _check_item_type(item)

    def filter_in_list(d, l):
        r = dict()
        for k, v in d.items():
            items_ = l.copy()
            for i in items_:
                if i not in v.columns:
                    items_.remove(i)
                    logger.warning("concept {} not found in ingredient {}".format(i, ingredient.id))
            r[k] = v[items_].copy()
        return r

    if item_type == 'list':
        res = filter_in_list(data, item)
    else:  # mongo
        selector = list(item.keys())[0]
        item_list = list(item.values())[0]
        if selector == '$in':
            res = filter_in_list(data, item_list)
        else:
            res = dict()
            for k, v in data.items():
                for i in item_list:
                    if i not in v.columns:
                        logger.warning("concept {} not found in ingredient {}".format(i, ingredient.id))
                keep_cols = list(set(v.columns.values) - set(item_list))
                res[k] = v[keep_cols].copy()
    return res


@debuggable
def filter(chef: Chef, ingredients: List[Ingredient], result, **options) -> Ingredient:
    """filter items and rows just as what `value` and `filter` do in ingredient definition.

    Procedure format:

    .. code-block:: yaml

       - procedure: filter
         ingredients:
             - ingredient_id
         options:
             item:  # just as `value` in ingredient def
                 $in:
                     - concept_1
                     - concept_2
             row:  # just as `filter` in ingredient def
                 $and:
                     geo:
                         $ne: usa
                     year:
                         $gt: 2010

         result: output_ingredient

    for more information, see the :py:class:`ddf_utils.chef.ingredient.Ingredient` class.

    Parameters
    ----------
    chef: Chef
        the Chef instance
    ingredients:
        list of ingredient id in the DAG
    result: `str`

    Keyword Args
    ------------
    item: list or dict, optional
        The item filter
    row: dict, optional
        The row filter
    """

    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info("filter_row: " + ingredient.id)

    row_filters = read_opt(options, 'row', False, None)
    items = read_opt(options, 'item', False, None)

    if row_filters is None and items is None:
        raise ProcedureError('filter procedure: at least one of `row` and `item` should be set in the options!')

    # first handel item filter, then row filter.
    # easily to see that the order doesn't affect the final result.
    if items is not None and len(items) != 0:
        if ingredient.dtype == 'datapoints':
            res = _filter_datapoint_item(ingredient, items)
        else:
            res = _filter_other_item(ingredient, items)
    else:
        res = dict()
        data = ingredient.get_data()
        for k, df in data.items():
            res[k] = df.copy()

    if row_filters is not None:
        for k, df in res.items():
            res[k] = query(df, row_filters, available_scopes=df.columns)

    return get_ingredient_class(ingredient.dtype).from_procedure_result(result, ingredient.key, data_computed=res)
