# -*- coding: utf-8 -*-
"""string functions for ddf files"""

import re
import numpy as np
from unidecode import unidecode


def to_concept_id(s, sep='_'):

    if s is np.nan:
        return s

    pattern = re.compile('[\W_]+')
    snew = unidecode(pattern.sub(sep, s.strip()).lower())

    # remove first/last underscore
    if snew[-1] == sep:
        snew = snew[:-1]
    if snew[0] == sep:
        snew = snew[1:]

    return snew


def fix_time_range(s):
    """change a time range to the middle of year in the range.
    e.g. fix_time_range('1980-90') = 1985
    """

    if '-' not in s:
        return int(s)

    else:
        t1, t2 = s.split('-')
        if len(t1) == 4 and len(t2) == 4:
            span = int(t2) - int(t1)
            return int(int(t1) + span / 2)
        else:  # t2 have only 1-2 digits.
            d = len(t2)
            hund1 = int(t1[:4-d])
            tens1 = int(t1[-d:])
            tens2 = int(t2)
            y1 = int(t1)
            if tens1 > tens2:
                hund2 = hund1 + 1
                y2 = hund2 * 10**d + tens2
            else:
                y2 = hund1 * 10**d + tens2

            return int(y1 + (y2 - y1) / 2 )


