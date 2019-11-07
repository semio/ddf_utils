# -*- coding: utf-8 -*-

"""A DSL for ETL
"""

# this lib is made to use as wildcard import
__all__ = ['has_duplicates']

import pandas as pd
import numpy as np

def has_duplicates(df, subset):
    """assert there is no duplicated key for a dataframe."""
    return np.any(df.duplicated(subset=subset))
