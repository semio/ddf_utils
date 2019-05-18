# -*- coding: utf-8 -*-

"""the DAG model of chef

The DAG consists of 2 types of nodes: IngredientNode and ProcedureNode.
each node will have a `evaluate()` function, which will return an ingredient
on eval.
"""

from . ingredient import Ingredient
from ..exceptions import ProcedureError, ChefRuntimeError
from ..helpers import get_procedure
import logging


class BaseNode:
    """The base node which IngredientNode and ProcedureNode inherit from

    Parameters
    ----------
    node_id : `str`
        the name of the node
    dag : DAG
        the `DAG` object the node is in
    """
    def __init__(self, node_id, chef):
        self.node_id = node_id
        self.chef = chef
        self.dag = chef.dag
        self._upstream_list = list()
        self._downstream_list = list()

    def __repr__(self):
        return '<Node {}>'.format(self.node_id)

    @property
    def upstream_list(self):
        return [self.dag.get_node(nid) for nid in self._upstream_list]

    @property
    def downstream_list(self):
        return [self.dag.get_node(nid) for nid in self._downstream_list]

    def add_upstream(self, node):
        self._upstream_list.append(node.node_id)

    def add_downstream(self, node):
        self._downstream_list.append(node.node_id)

    def evaluate(self):
        raise NotImplementedError('')

    def get_direct_relatives(self, upstream=False):
        """
        Get the direct relatives to the current node, upstream or
        downstream.
        """
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def detect_missing_dependency(self):
        """
        check if every upstream is available in the DAG.
        raise error if something is missing
        """
        not_found = set()
        for n in self.upstream_list:
            if not self.dag.has_node(n.node_id):
                not_found.add(n.node_id)
            if isinstance(n, ProcedureNode) and not n.procedure:
                not_found.add(n.node_id)
        if len(not_found) > 0:
            raise ChefRuntimeError(
                "dependency not found/not defined for {}: {}".format(self.node_id, not_found))
        return False


class IngredientNode(BaseNode):
    """Node for storing dataset ingredients.

    Parameters
    ----------
    ingredient : Ingredient
        the ingredient in this node
    """
    def __init__(self, node_id, ingredient, chef):
        super(IngredientNode, self).__init__(node_id, chef)
        self.ingredient = ingredient

    def evaluate(self) -> Ingredient:
        """return the ingredient as is"""
        logging.debug("evaluating {}".format(self.node_id))
        return self.ingredient


class ProcedureNode(BaseNode):
    """The node for storing procedure results

    The evaluate() function will run a procedure according to `self.procedure`, using
    other nodes' data. Other nodes will be evaluated if when necessary.

    Parameters
    ----------
    procedure : dict
        the procedure dictionary
    """
    def __init__(self, node_id, procedure, chef):
        super(ProcedureNode, self).__init__(node_id, chef)
        self.procedure = procedure
        self.result_ingredient = None

    def evaluate(self) -> Ingredient:
        logging.debug("evaluating {}".format(self.node_id))
        if self.result_ingredient is not None:
            if self.result_ingredient.get_data() is not None:
                return self.result_ingredient

        # get the procedure function, raise error if procedure not supported
        # supported format:
        # procedure: sub/dir/module.function
        # procedure: module.function
        try:
            func = get_procedure(self.procedure['procedure'], self.chef.config.get('procedure_dir', None))
        except (AttributeError, ImportError):
            raise ProcedureError("No such procedure: " + self.procedure['procedure'])
        except TypeError:
            raise ProcedureError("Procedure Error: " + str(self.node_id))

        ingredients = [self.dag.get_node(x).evaluate() for x in self.procedure['ingredients']]
        if 'options' in self.procedure.keys():
            options = self.procedure['options']
            self.result_ingredient = func(self.chef, ingredients, result=self.procedure['result'], **options)
        else:
            self.result_ingredient = func(self.chef, ingredients, result=self.procedure['result'])

        return self.result_ingredient


class DAG:
    """The DAG model.

    A dag (directed acyclic graph) is a collection of tasks with directional
    dependencies. DAGs essentially act as namespaces for its nodes. A node_id
    can only be added once to a DAG.
    """
    def __init__(self, node_dict=None):
        if not node_dict:
            self._node_dict = dict()
        else:
            self._node_dict = node_dict

    def copy(self):
        from copy import deepcopy
        # TODO: I think we should add copy() for Nodes
        return DAG(node_dict=deepcopy(self._node_dict))

    @property
    def roots(self):
        """return the roots of the DAG"""
        return [t for t in self.nodes if not t.downstream_list]

    @property
    def nodes(self):
        """return all nodes"""
        return list(self.node_dict.values())

    @property
    def node_dict(self):
        return self._node_dict

    @node_dict.setter
    def node_dict(self, node):
        raise AttributeError('can not set node_dict manually')

    def add_node(self, node):
        """add a node to DAG"""
        if node.node_id in self.node_dict.keys():
            # only overwirte case is when procedure in ProcedureNode is None.
            if (isinstance(node, ProcedureNode) and
                not self.node_dict[node.node_id].procedure):
                self.node_dict[node.node_id] = node
            else:
                raise ChefRuntimeError('can not overwirte node already exists: ' + node.node_id)
        self.node_dict[node.node_id] = node

    def get_node(self, node_id):
        if node_id in self.node_dict.keys():
            return self.node_dict[node_id]
        raise ChefRuntimeError('node {} not found'.format(node_id))

    def has_node(self, node_id):
        return node_id in self.node_dict.keys()

    def add_dependency(self, upstream_node_id, downstream_node_id):
        """
        Simple utility method to set dependency between two nodes that
        already have been added to the DAG using add_node()
        """
        self.get_node(upstream_node_id).add_downstream(
            self.get_node(downstream_node_id))
        self.get_node(downstream_node_id).add_upstream(
            self.get_node(upstream_node_id))

    def tree_view(self):
        """
        Shows an ascii tree representation of the DAG
        """
        cycles = self.detect_cycles()
        if cycles:
            print("cycle detected: {}".format(cycles))
            return

        def get_downstream(node, level=0):
            print((" " * level * 4) + str(node))
            level += 1
            for t in node.upstream_list:
                get_downstream(t, level)

        for t in self.roots:
            get_downstream(t)

    def detect_cycles(self):
        """Detect cycles in DAG, following `Tarjan's algorithm`_.

        .. _`Tarjan's algorithm`: https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
        """
        index_counter = [0]
        stack = []
        lowlinks = {}
        index = {}
        result = []

        def strongconnect(node: BaseNode):
            # set the depth index for this node to the smallest unused index
            node_id = node.node_id
            index[node_id] = index_counter[0]
            lowlinks[node_id] = index_counter[0]
            index_counter[0] += 1
            stack.append(node)

            successors = node.upstream_list
            for successor in successors:
                if successor.node_id not in lowlinks:
                    # Successor has not yet been visited; recurse on it
                    strongconnect(successor)
                    lowlinks[node_id] = min(lowlinks[node_id], lowlinks[successor.node_id])
                elif successor in stack:
                    # the successor is in the stack and hence in the current strongly
                    # connected component (SCC)
                    lowlinks[node_id] = min(lowlinks[node_id], index[successor.node_id])
            # If `node` is a root node, pop the stack and generate an SCC
            if lowlinks[node_id] == index[node_id]:
                connected_component = []

                while True:
                    successor = stack.pop()
                    connected_component.append(successor.node_id)
                    if successor == node:
                        break
                component = tuple(connected_component)
                # storing the result
                result.append(component)

        for node in self.nodes:
            if node.node_id not in lowlinks:
                strongconnect(node)

        return list(filter(lambda x: len(x) > 1, result))
