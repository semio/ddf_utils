"""
tests for vcs functions
"""

import os
import tempfile
from ddf_utils.vcs.base import (get_url_scheme, is_url, get_rev,
                                local_path_from_url,
                                local_path_from_requirement
                                )


def test_uri_from_requirement():
    # TODO: maybe add support for these urls
    # git+open-numbers/ddf--gapminder--wdi
    # git+git@github.com:open-numbers/ddf--gapminder--wdi
    to_test = [
        'git+ssh://github.com/open-numbers/ddf--gapminder--wdi',
        'git+https://github.com/open-numbers/ddf--gapminder--wdi',
        'git+file:///home/user/git/open-numbers/ddf--gapminder--wdi',
        'git+https://github.com/open-numbers/ddf--gapminder--wdi@master',
        'git+https://github.com/open-numbers/ddf--gapminder--wdi@v1.0',
        'git+https://github.com/open-numbers/ddf--gapminder--wdi@da39a3ee5e6b4b0d3255bfef95601890afd80709',
        'git+https://github.com/open-numbers/ddf--gapminder--wdi@refs/pull/123/head'
    ]

    for t in to_test:
        assert is_url(t)

    revs = [get_rev(t) for t in to_test]
    assert revs == ['master', 'master', 'master', 'master',
                    'v1.0', 'da39a3ee5e6b4b0d3255bfef95601890afd80709',
                    'refs/pull/123/head']


def test_local_path_from_url():
    dataset_path = '/tmp/datasets'
    url = 'git+https://github.com/open-numbers/ddf--gapminder--wdi@v1.0'
    assert local_path_from_url(url, dataset_path) == \
        os.path.join(dataset_path,
                     'repos/github.com/open_numbers/ddf--gapminder--wdi')

    url = 'git+https://github.com/open-numbers/ddf--gapminder--wdi'
    assert local_path_from_url(url, dataset_path) == \
        os.path.join(dataset_path,
                     'repos/github.com/open_numbers/ddf--gapminder--wdi')


def test_local_path_from_requirement():
    dataset_path = '/tmp/datasets'
    name = 'github.com/open-numbers/ddf--gapminder--wdi@v1.0'
    assert local_path_from_requirement(url, dataset_path) == \
        os.path.join(dataset_path,
                     'repos/github.com/open_numbers/ddf--gapminder--wdi')

    url = 'git+https://github.com/open-numbers/ddf--gapminder--wdi'
    assert local_path_from_requirement(url, dataset_path) == \
        os.path.join(dataset_path,
                     'repos/github.com/open_numbers/ddf--gapminder--wdi')
