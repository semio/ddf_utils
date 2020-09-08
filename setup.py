# -*- coding: utf-8 -*-

import os
import sys

from setuptools import find_packages, setup

version = "1.0.10"

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
    'Click',
    'daff',
    'tabulate',
    'dask[dataframe]',
    'tqdm',
    'ruamel.yaml',
    'graphviz',
    'coloredlogs',
    'pytz',
    'requests[security]',
    'gitpython',
    'hy',
    'attrs>=19.2.0',
    'joblib',
    'lxml'
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
        # "Programming Language :: Python :: 3.3",
        # "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Utilities"
    ]
)
