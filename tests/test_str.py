# -*- coding: utf-8 -*-


import re
import pandas as pd
from hypothesis import given, strategies as st


@given(s=st.text(min_size=1))
def test_to_concept_id(s):
    from ddf_utils.str import to_concept_id

    res = to_concept_id(s)
    if res:
        assert re.match('[0-9a-z_]*', res)


@given(num=st.floats())
def test_format_float_sigfig(num):
    from ddf_utils.str import format_float_sigfig

    res = format_float_sigfig(num, 5)

    if not pd.isnull(res):
        assert re.match('[0-9.e\+]*', res)
