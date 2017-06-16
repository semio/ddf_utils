# -*- coding: utf-8 -*-
"""functions for datapackage.json"""

import os
import re
import json
import csv
import logging
from .model.package import Datapackage
from collections import OrderedDict


def get_datapackage(path, use_existing=True, update=True):
    """get the datapackage.json from a dataset path, create one if it's not exists

    Parameters
    ----------
    path : `str`
        the dataset path

    Keyword Args
    ------------
    use_existing : bool
        whether or not to use the existing datapackage
    """
    datapackage_path = os.path.join(path, 'datapackage.json')

    if os.path.exists(datapackage_path):
        with open(datapackage_path, encoding='utf8') as f:
            datapackage_old = json.load(f, object_pairs_hook=OrderedDict)

        if use_existing:
            if not update:
                return datapackage_old
            try:
                datapackage_old.pop('resources')  # don't use the old resources
                datapackage_old.pop('ddfSchema')  # and ddf schema
            except KeyError:
                logging.warning('no resources or ddfSchema in datapackage.json')
            datapackage_new = create_datapackage(path, **datapackage_old)
        else:
            datapackage_new = create_datapackage(path)
    else:
        if use_existing:
            print("WARNING: no existing datapackage.json")
        datapackage_new = create_datapackage(path)

    return datapackage_new


def get_ddf_files(path, root=None):
    info = next(os.walk(path))

    # don't include hidden and lang/etl dir.
    sub_dirs = [
        x for x in info[1] if (not x.startswith('.') and x not in ['lang', 'etl', 'langsplit'])
    ]
    files = list()
    for x in info[2]:
        if x.startswith('ddf--') and x != 'ddf--index.csv' and x.endswith('.csv'):
            files.append(x)
        else:
            logging.warning('skipping file {}'.format(x))

    for f in files:
        if root:
            yield os.path.join(root, f)
        else:
            yield f

    for sd in sub_dirs:
        for p in get_ddf_files(os.path.join(path, sd), root=sd):
            yield p


def create_datapackage(path, gen_schema=True, **kwargs):
    """create datapackage.json base on the files in `path`.

    If you want to set some attributes manually, you can pass them as
    keyword arguments to this function

    Note
    ----
    A DDFcsv datapackage MUST contain the fields `name` and `resources`.

    if name is not provided, then the base name of `path` will be used.

    Parameters
    ----------
    path : `str`
        the dataset path to create datapackage.json
    """

    datapackage = OrderedDict()

    # setting default name / lang
    try:
        name = kwargs.pop('name')
    except KeyError:
        # print('name not specified, using the path name')
        name = os.path.basename(os.path.normpath(os.path.abspath(path)))
    try:
        lang = kwargs.pop('language')
    except KeyError:
        lang = {'id': 'en'}

    datapackage['name'] = name
    datapackage['language'] = lang

    # add all optional settings
    for k in sorted(kwargs.keys()):
        datapackage[k] = kwargs[k]

    # generate resources
    resources = []
    names_sofar = dict()

    for f in get_ddf_files(path):
        path_res = f
        name_res = os.path.splitext(os.path.basename(f))[0]

        if name_res in names_sofar.keys():
            names_sofar[name_res] = names_sofar[name_res] + 1
            # adding a tail to the recource name, because it should be unique
            name_res = name_res + '-' + str(names_sofar[name_res])
        else:
            names_sofar[name_res] = 0

        resources.append(OrderedDict([('path', path_res), ('name', name_res)]))

    # TODO: make separate functions. this function is too long.
    for n, r in enumerate(resources):
        name_res = r['name']
        schema = {"fields": [], "primaryKey": None}

        if 'datapoints' in name_res:
            conc, keys = re.match('ddf--datapoints--([\w_]+)--by--(.*)', name_res).groups()
            primary_keys = keys.split('--')
            # print(conc, primary_keys)
            for i, k in enumerate(primary_keys):
                if '-' in k:
                    k_new, *enums = k.split('-')
                    primary_keys[i] = k_new
                    constraint = {'enum': enums}
                    schema['fields'].append({'name': k_new, 'constraints': constraint})
                else:
                    schema['fields'].append({'name': k})

            schema['fields'].append({'name': conc})
            schema['primaryKey'] = primary_keys

            resources[n].update({'schema': schema})

        elif 'entities' in name_res:
            match = re.match('ddf--entities--([\w_]+)(--[\w_]*)?-?.*', name_res).groups()
            domain, concept = match
            if concept is not None:
                concept = concept[2:]

            with open(os.path.join(path, r['path'])) as f:
                reader = csv.reader(f, delimiter=',', quotechar='"')
                # we only need the headers for index file
                header = next(reader)

            if domain in header:
                key = domain
            elif concept is not None and concept in header:
                key = concept
            else:
                raise ValueError('no header in {} matches its implied domain/entity_set!'.format(name_res))
                # print(
                #     """There is no matching header found for {}. Using the first column header
                #     """.format(name_res)
                # )
                # key = header[0]

            schema['primaryKey'] = key
            for h in header:
                schema['fields'].append({'name': h})
            resources[n].update({'schema': schema})

        elif 'concepts' in name_res:
            with open(os.path.join(path, r['path'])) as f:
                reader = csv.reader(f, delimiter=',', quotechar='"')
                header = next(reader)
            schema['primaryKey'] = 'concept'
            for h in header:
                schema['fields'].append({'name': h})

            resources[n].update({'schema': schema})
        else:  # not entity/concept/datapoint. it's not supported yet so we don't include them.
            print("not supported file: " + name_res)
            resources[n] = None

    datapackage['resources'] = [x for x in resources if x is not None]

    # generate ddf schema
    if gen_schema:
        dp = Datapackage(datapackage, base_dir=path)
        logging.info('generating ddf schema, may take some time...')
        dp.generate_ddfschema()

        return dp.datapackage
    else:
        return datapackage


# helper for dumping datapackage json
def dump_json(path, obj):
    with open(path, 'w+') as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)
        f.close()
