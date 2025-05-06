# -*- coding: utf-8 -*-

import os
import sys

from setuptools import find_packages, setup

version = "1.0.16"

if sys.argv[-1] == 'tag':
    os.system("git tag -a %s -m 'version %s'" % (version, version))
    os.system("git push --tags")
    sys.exit()

requirements = [
    'pandas~=2.2.3',
    'unidecode~=1.4.0',
    'pyyaml~=6.0.2',
    'orderedattrdict~=1.6.0',
    'typing~=3.7.4.3',
    'cookiecutter~=2.6.0',
    'Click~=8.1.8',
    'daff~=1.4.2',
    'tabulate~=0.9.0',
    'dask[dataframe]~=2025.4.1',
    'tqdm~=4.67.1',
    'ruamel.yaml~=0.18.10',
    'graphviz~=0.20.3',
    'coloredlogs~=15.0.1',
    'pytz~=2025.2',
    'requests[security]~=2.32.3',
    'gitpython~=3.1.44',
    'hy~=1.0.0',
    'attrs~=25.3.0',
    'joblib~=1.5.0',
    'lxml~=5.4.0',
    'pycurl~=7.45.6'
]

setup(
    name='ddf_utils',
    version=version,
    description='Commonly used functions/utilities for DDF file model.',
    long_description='README.md',
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
    },
    keywords=['etl', 'ddf', 'datasets', 'recipe'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Utilities"
    ]
)
