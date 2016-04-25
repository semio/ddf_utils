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
