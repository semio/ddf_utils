# -*- coding: utf-8 -*-

"""functions working with patches"""

import pandas as pd
import daff


def apply_patch(base, patch):
    """apply patch created with daff.
    more on the diff format, see: http://specs.frictionlessdata.io/tabular-diff-format/

    return: the updated DataFrame.
    """
    d = daff.Coopy()

    base_df = pd.read_csv(base, header=None)
    patch_df = pd.read_csv(patch, header=None)

    base_list = base_df.values.tolist()
    if d.patch(base_list, patch_df.values.tolist()):
        new_df = pd.DataFrame(base_list[1:], columns=base_list[0])
    else:
        raise ValueError("patch failed. double check you patch")

    return new_df
