# -*- coding: utf-8 -*-

"""commonly used calculation methods"""

import pandas as pd
import numpy as np


# Groupby Aggrgate


# Groupby Transform
def zcore(x):
    return (x - x.mean()) / x.std()


# Groupby Filter
def gt(x, val, how='all', include_eq=False):
    f = getattr(np, how)
    if include_eq:
        return f(x >= val)
    else:
        return f(x > val)


def lt(x, val, how='all', include_eq=False):
    f = getattr(np, how)
    if include_eq:
        return f(x <= val)
    else:
        return f(x <= val)


def between(x, upper, lower, how='all', include_upper=False, include_lower=False):
    return gt(x, lower, how, include_lower) and lt(x, upper, how, include_upper)


# Rolling
def aagr(df: pd.DataFrame, window: int=10):  # TODO: don't include the window
    """average annual growth rate

    Parameters
    ----------
    window : `int`
        the rolling window size

    Returns
    -------
    return : `DataFrame`
        The rolling apply result
    """
    pct = df.pct_change()
    return pct.rolling(window).apply(np.mean).dropna()
