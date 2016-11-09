# -*- coding: utf-8 -*-
"""io functions for ddf files"""

import os
# import pandas as pd


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


def cleanup(path):
    """remove all ddf files in the given path"""
    for f in os.listdir(path):
        if f.startswith("ddf--"):
            os.remove(os.path.join(path, f))
