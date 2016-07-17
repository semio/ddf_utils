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


