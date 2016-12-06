# -*- coding: utf-8 -*-

"""the DAG module of chef"""

import pandas as pd
from . procedure import *

# supported procedures
supported_procs = {
    'translate_column': translate_column,
    'translate_header': translate_header,
    'identity': identity,
    'merge': merge,
    'run_op': run_op,
    'filter_row': filter_row,
    'align': align,
    'filter_item': filter_item,
    'groupby': groupby,
    'accumulate': accumulate,
    'copy': copy,
    'extract_concepts': extract_concepts
}


class BaseNode():
    def __init__(self, node_id, dag):
        self.node_id = node_id
        self.dag = dag
        self._upstream_list = list()
        self._downstream_list = list()

    def __repr__(self):
        return '<Node {}>'.format(self.node_id)

    @property
    def upstream_list(self):
        return [self.dag.get_task(nid) for nid in self._upstream_list]

    @property
    def downstream_list(self):
        return [self.dag.get_task(nid) for nid in self._downstream_list]

    def add_upstream(self, node):
        self._upstream_list.append(node.node_id)

    def add_downstream(self, node):
        self._downstream_list.append(node.node_id)

    def evaluate(self):
        raise NotImplementedError('')

    def get_direct_relatives(self, upstream=False):
        """
        Get the direct relatives to the current task, upstream or
        downstream.
        """
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def detect_downstream_cycle(self, task=None):
        """
        When invoked, this routine will raise an exception if a cycle is
        detected downstream from self. It is invoked when tasks are added to
        the DAG to detect cycles.
        """
        if not task:
            task = self
        for t in self.get_direct_relatives():
            if task is t:
                msg = "Cycle detected in DAG. Faulty task: {0}".format(task)
                raise ValueError(msg)
            else:
                t.detect_downstream_cycle(task=task)
        return False


class IngredientNode(BaseNode):
    def __init__(self, node_id, ingredient, dag):
        super(IngredientNode, self).__init__(node_id, dag)
        self.ingredient = ingredient

    def evaluate(self):
        return self.ingredient


class ProcedureNode(BaseNode):
    def __init__(self, node_id, procedure, dag):
        super(ProcedureNode, self).__init__(node_id, dag)
        self.procedure = procedure
        self.result_ingredient = None

    def evaluate(self):
        if self.result_ingredient:
            return self.result_ingredient
        funcs = supported_procs
        func = self.procedure['procedure']

        # raise error if procedure not supported
        if func not in funcs.keys() and func != 'serve':
            raise NotImplementedError("Not supported: " + func)

        # check the base ingredients and convert the string id to actual ingredient
        ingredients = []
        for i in self.procedure['ingredients']:
            ing = self.dag.get_task(i)
            ingredients.append(ing.evaluate())

        # also evaluate the ingredients in the options
        if 'options' in self.procedure.keys():
            options = self.procedure['options']
            if 'base' in options.keys():
                ing = self.dag.get_task(self.procedure['options']['base'])
                options['base'] = ing.evaluate()
            for opt in options.keys():
                if isinstance(options[opt], dict):
                    if 'base' in options[opt].keys():
                        ing = self.dag.get_task(options[opt]['base'])
                        options[opt]['base'] = ing.evaluate()
        else:
            options = dict()

        self.result_ingredient = funcs[func](*ingredients, result=self.procedure['result'], **options)
        return self.result_ingredient


class DAG():
    def __init__(self, task_dict=None):
        if not task_dict:
            self._task_dict = dict()
        else:
            self._task_dict = task_dict

    @property
    def roots(self):
        return [t for t in self.tasks if not t.downstream_list]

    @property
    def tasks(self):
        return list(self.task_dict.values())

    @property
    def task_dict(self):
        return self._task_dict

    @task_dict.setter
    def task_dict(self, task):
        raise AttributeError('can not set task_dict manually')

    def add_task(self, task):
        if task.node_id in self.task_dict.keys():
            # only overwirte case is when procedure in ProcedureNode is None.
            if (isinstance(task, ProcedureNode) and
                not self.task_dict[task.node_id].procedure):
                self.task_dict[task.node_id] = task
            else:
                raise ValueError('can not overwirte node already exists: ' + task.node_id)
        self.task_dict[task.node_id] = task

    def get_task(self, task_id):
        if task_id in self.task_dict.keys():
            return self.task_dict[task_id]
        raise ValueError('task {} not found'.format(task_id))

    def has_task(self, task_id):
        return task_id in self.task_dict.keys()

    def add_dependency(self, upstream_task_id, downstream_task_id):
        """
        Simple utility method to set dependency between two tasks that
        already have been added to the DAG using add_task()
        """
        self.get_task(upstream_task_id).add_downstream(
            self.get_task(downstream_task_id))
        self.get_task(downstream_task_id).add_upstream(
            self.get_task(upstream_task_id))

    def tree_view(self):
        """
        Shows an ascii tree representation of the DAG
        """
        def get_downstream(task, level=0):
            print((" " * level * 4) + str(task))
            level += 1
            for t in task.upstream_list:
                get_downstream(t, level)

        for t in self.roots:
            get_downstream(t)
