# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os
from collections import OrderedDict


def to_dict_dropna(data):
    """return a dictionary that do not contain any NaN values from a dataframe."""
    return dict((k, v.dropna().to_dict()) for k, v in data.iterrows())


def generate_metadata(ddf_path, outpath):

    try:
        ddf_concept = pd.read_csv(os.path.join(ddf_path, 'ddf--concepts.csv'))
    except:
        ddf_concept = pd.read_csv(os.path.join(ddf_path, 'ddf--concepts--continous.csv'))

    # use OrderedDict in order to keep the order of insertion.
    indb = OrderedDict([['indicatorsDB', OrderedDict()]])

    # TODO:
    # 1. geo concepts?
    # 2. oneset?

    # TODO: see if below columns are engough
    measure_cols = ['concept', 'sourceLink', 'scales', 'interpolation', 'color']

    mdata = ddf_concept[ddf_concept['concept_type'] == 'measure'][measure_cols]
    mdata = mdata.set_index('concept')
    mdata = mdata.drop(['longitude', 'latitude'])
    mdata.columns = ['sourceLink', 'scales', 'interpolation', 'color']
    mdata['use'] = 'indicator'

    mdata_dict = to_dict_dropna(mdata)
    for k in sorted(mdata_dict.keys()):
        indb['indicatorsDB'][k] = mdata_dict.get(k)

    for i in indb['indicatorsDB'].keys():
        fname = os.path.join(ddf_path, 'ddf--datapoints--'+i+'--by--geo--time.csv')
        try:
            df = pd.read_csv(fname, dtype={i: float, 'time': int})
        except (OSError, IOError):
            print('no datapoints for ', i)
            continue

        # domain and availability
        dm = [float(df[i].min()), float(df[i].max())]
        av = [int(df['time'].min()), int(df['time'].max())]

        # domain_quantiles_10_90:
        # 1) sort by indicator value
        # 2) remove top and bottom 10% of values (os if 100 points, remove 10 from top and bottom)
        # 3) take first and last value of what's left as min and max in the property above.
        values_sorted = df[i].sort_values().values
        q_10 = np.round(len(values_sorted) / 10)
        q_90 = -1 * q_10 - 1

        # values_sorted = values_sorted[q_10:q_90]
        # domain_quantiles_10_90 = [values_sorted.min(), values_sorted.max()]

        domain_quantiles_10_90 = [values_sorted[q_10], values_sorted[q_90]]

        indb['indicatorsDB'][i].update({
            'domain': dm, 'availability': av,
            'domain_quantiles_10_90': domain_quantiles_10_90
        })

    return indb['indicatorsDB']
