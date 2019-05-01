# -*- coding: utf-8 -*-

import os
from ddf_utils.patch import apply_patch


def test_patch():
    input_dir = os.path.join(os.path.dirname(__file__), 'chef/csvs')

    file1 = os.path.join(input_dir, 'external.csv')
    file2 = os.path.join(input_dir, 'patch.csv')

    output = apply_patch(file1, file2)

    assert 'col3' in output.concept.values
    assert 'ColX' in output.loc[output.concept == 'col1', 'name'].values
