# -*- coding: utf-8 -*-
"""io functions for ddf files"""

import os
import shutil
import pandas as pd


def to_csv(df, out_dir, ftype, concept, by=None, **kwargs):
    """save a ddf dataframe to csv file.
    the file path of csv will be out_dir/ddf--$ftype--$concept--$by.csv
    """

    if not by:
        path = os.path.join(out_dir, 'ddf--'+ftype+'--'+concept+'.csv')
    else:
        if isinstance(by, list):
            filename = 'dff--' + '--'.join([ftype, concept]) + '--'.join(by) + '.csv'
        else:
            filename = 'dff--' + '--'.join([ftype, concept, by]) + '.csv'

        path = os.path.join(out_dir, filename)

    df.to_csv(path, **kwargs)


def load_google_xls(filehash):
    # TODO: return the xls file with given filehash
    raise NotImplementedError


def cleanup(path, how='ddf'):
    """remove all ddf files in the given path"""
    # TODO: support names don't have standard ddf name format.
    if how == 'ddf':
        for f in os.listdir(path):
            if f.startswith("ddf--"):
                os.remove(os.path.join(path, f))
        if os.path.exists(os.path.join(path, 'datapackage.json')):
            os.remove(os.path.join(path, 'datapackage.json'))
    if how == 'lang':
        if os.path.exists(os.path.join(path, 'lang')):
            shutil.rmtree(os.path.join(path, 'lang'))
    if how == 'langsplit':
        if os.path.exists(os.path.join(path, 'langsplit')):
            shutil.rmtree(os.path.join(path, 'langsplit'))


def csvs_to_ddf(files, out_path):
    """convert raw files to ddfcsv

    Args
    ----
    files: list
        a list of file paths to build ddf csv
    out_path: `str`
        the directory to put the ddf dataset

    """
    import re
    from os.path import join
    from ddf_utils.str import to_concept_id
    from ddf_utils.datapackage import get_datapackage

    concepts_df = pd.DataFrame([['name', 'Name', 'string']],
                               columns=['concept', 'name', 'concept_type'])
    concepts_df = concepts_df.set_index('concept')

    all_entities = dict()

    pattern = 'indicators--by--([ 0-9a-zA-Z_-]*).csv'

    for f in files:
        data = pd.read_csv(f)
        basename = os.path.basename(f)
        keys = re.match(pattern, basename).groups()[0].split('--')
        keys_alphanum = list(map(to_concept_id, keys))

        # check if there is a time column. Assume last column is time.
        try:
            pd.to_datetime(data[keys[-1]], format='%Y')
        except (ValueError, pd.tslib.OutOfBoundsDatetime):
            has_time = False
        else:
            has_time = True

        if has_time:
            ent_keys = keys[:-1]
        else:
            ent_keys = keys

        # set concept type
        for col in data.columns:
            concept = to_concept_id(col)

            if col in keys:
                if col in ent_keys:
                    t = 'entity_domain'
                else:
                    t = 'time'
            else:
                t = 'measure'

            concepts_df.ix[concept] = [col, t]

        for ent in ent_keys:
            ent_df = data[[ent]].drop_duplicates().copy()
            ent_concept = to_concept_id(ent)
            ent_df.columns = ['name']
            ent_df[ent_concept] = ent_df.name.map(to_concept_id)

            if ent_concept not in all_entities.keys():
                all_entities[ent_concept] = ent_df
            else:
                all_entities[ent_concept] = pd.concat([all_entities[ent_concept], ent_df],
                                                      ignore_index=True)

        data = data.set_index(keys)
        for c in data:
            # output datapoints
            df = data[c].copy()
            df = df.reset_index()
            for k in keys[:-1]:
                df[k] = df[k].map(to_concept_id)
            df.columns = df.columns.map(to_concept_id)
            (df.dropna()
               .to_csv(join(out_path,
                            'ddf--datapoints--{}--by--{}.csv'.format(
                                to_concept_id(c), '--'.join(keys_alphanum))),
                       index=False))

    # output concepts
    concepts_df.to_csv(join(out_path, 'ddf--concepts.csv'))

    # output entities
    for c, df in all_entities.items():
        df.to_csv(join(out_path, 'ddf--entities--{}.csv'.format(c)), index=False)

    _ = get_datapackage(out_path, to_disk=True, use_existing=False)

    return
