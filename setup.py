# -*- coding: utf-8 -*-

import sys
import os
from setuptools import setup, find_packages

version = "0.2.12"

if sys.argv[-1] == 'tag':
    os.system("git tag -a %s -m 'version %s'" % (version, version))
    os.system("git push --tags")
    sys.exit()

requirements = [
    'pandas',
    'unidecode',
    'pyyaml',
    'orderedattrdict',
    'typing',
    'cookiecutter',
    'jsonschema',
    'Click',
    'daff'
]

setup(
    name='ddf_utils',
    version=version,
    description='Commonly used functions/utilities for DDF file model.',
    url='https://github.com/semio/ddf_utils',
    author='Semio Zheng',
    author_email='prairy.long@gmail.com',
    license='MIT',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'res']),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'ddf = ddf_utils.cli:ddf'
        ]
    }
)
