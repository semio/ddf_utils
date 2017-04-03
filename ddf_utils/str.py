# -*- coding: utf-8 -*-
"""string functions for ddf files"""

import re
import pandas as pd
from unidecode import unidecode
import decimal


def to_concept_id(s, sep='_'):
    """convert a string to alphanumeric format."""
    if pd.isnull(s):
        return s
    # replace some symbol to meaningful strings
    sub = {'%': 'pct', '>': 'gt', '<': 'lt'}
    for k, v in sub.items():
        if k in s:
            s = s.replace(k, v)

    pattern = re.compile('[\W_]+')  # match all Non-alphanumeric
    snew = unidecode(s.strip())
    snew = pattern.sub(sep, snew).lower()

    if len(snew.replace(sep, '')) == 0:
        return None

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
            hund1 = int(t1[:4 - d])
            tens1 = int(t1[-d:])
            tens2 = int(t2)
            y1 = int(t1)
            if tens1 > tens2:
                hund2 = hund1 + 1
                y2 = hund2 * 10**d + tens2
            else:
                y2 = hund1 * 10**d + tens2

            return int(y1 + (y2 - y1) / 2)


def _float_to_decimal(f):
    """Convert a floating point number to a Decimal with no loss of information

    see http://docs.python.org/library/decimal.html#decimal-faq
    """
    n, d = f.as_integer_ratio()
    numerator, denominator = decimal.Decimal(n), decimal.Decimal(d)
    ctx = decimal.Context(prec=60)
    result = ctx.divide(numerator, denominator)
    while ctx.flags[decimal.Inexact]:
        ctx.flags[decimal.Inexact] = False
        ctx.prec *= 2
        result = ctx.divide(numerator, denominator)
    return result


def format_float_digits(number, digits=5, threshold=None, keep_decimal=False):
    """format the number to string, limit the maximum amount of digits. Removing tailing zeros."""
    # assert(digits > 0)
    if pd.isnull(number):
        return number
    try:
        d = decimal.Decimal(number)
    except TypeError:
        d = _float_to_decimal(float(number))

    if threshold:
        if abs(d) <= threshold:
            return '0'

    s = format(d, '.{}f'.format(digits))

    if '.' in s:
        s = s.rstrip('0')
        if s[-1] == '.':
            if keep_decimal:
                s = s + '0'  # keep the decimal point and one zero.
            else:
                s = s[:-1]
            if s.startswith("-0"):
                s = "0"
    return s


def format_float_sigfig(number, sigfig=5, threshold=None):
    """format the number to string, keeping some significant digits."""
    # http://stackoverflow.com/questions/2663612/nicely-representing-a-floating-point-number-in-python/2663623#2663623
    # assert(sigfig > 0)
    if pd.isnull(number):
        return number
    try:
        d = decimal.Decimal(number)
    except TypeError:
        d = _float_to_decimal(float(number))

    if threshold:
        if abs(d) <= threshold:
            d = decimal.Decimal(0)

    sign, digits, exponent = d.as_tuple()

    if len(digits) < sigfig:
        digits = list(digits)
        digits.extend([0] * (sigfig - len(digits)))
    shift = d.adjusted()
    result = int(''.join(map(str, digits[:sigfig])))
    # Round the result
    if len(digits) > sigfig and digits[sigfig] >= 5:
        result += 1
    result = list(str(result))
    # Rounding can change the length of result
    # If so, adjust shift
    shift += len(result) - sigfig
    # reset len of result to sigfig
    result = result[:sigfig]
    if shift >= sigfig - 1:
        # Tack more zeros on the end
        result += ['0'] * (shift - sigfig + 1)
    elif 0 <= shift:
        # Place the decimal point in between digits
        result.insert(shift + 1, '.')
    else:
        # Tack zeros on the front
        assert (shift < 0)
        result = ['0.'] + ['0'] * (-shift - 1) + result
    if sign:
        result.insert(0, '-')
    return ''.join(result)
