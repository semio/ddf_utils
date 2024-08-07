# -*- coding: utf-8 -*-


import re
import pandas as pd
from hypothesis import given, strategies as st
import hypothesis


@hypothesis.settings(deadline=500)
@given(s=st.text(min_size=0))
def test_to_concept_id(s):
    from ddf_utils.str import to_concept_id

    res = to_concept_id(s)
    if res:
        assert re.match(r'[0-9a-z_]*', res)


@hypothesis.settings(deadline=500)
@given(num=st.floats())
def test_format_float_sigfig(num):
    from ddf_utils.str import format_float_sigfig

    res1 = format_float_sigfig(num, 5)

    if not pd.isnull(res1):
        assert re.match(r'[0-9.e\+]*', res1)

    res2 = format_float_sigfig(str(num), 5)

    if not pd.isnull(res2):
        assert re.match(r'[0-9.e\+]*', res2)


@hypothesis.settings(deadline=500)
@given(num=st.floats())
def test_format_float_digits(num):
    from ddf_utils.str import format_float_digits

    res1 = format_float_digits(num, 5)

    if not pd.isnull(res1):
        assert re.match(r'[0-9.e\+]*', res1)

    res2 = format_float_digits(str(num), 5, keep_decimal=False)

    if not pd.isnull(res2):
        assert re.match(r'[0-9]*', res2)


@hypothesis.settings(deadline=500)
@given(num=st.floats(min_value=0.1, max_value=10e13))
def test_float_to_decimal(num):
    from ddf_utils.str import _float_to_decimal

    _float_to_decimal(num)


def test_fix_time_range():
    from ddf_utils.str import fix_time_range

    assert fix_time_range('1990') == 1990
    assert fix_time_range('1980-90') == 1985
    assert fix_time_range('1980-1990') == 1985
