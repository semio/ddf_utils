# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name='ddf_utils',
    version='0.1.2-dev',
    description='Commonly used functions/utilities for DDF file model.',
    url='https://github.com/semio/ddf_utils',
    author='Semio Zheng',
    author_email='prairy.long@gmail.com',
    license='MIT',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'scripts']),
    install_requires=['pandas', 'unidecode', 'pyyaml', 'orderedattrdict', 'typing',
                      'cookiecutter', 'Click'],
    scripts=['scripts/runrecipe.py', 'scripts/ddfnew.py', 'scripts/ddf.py']
)
