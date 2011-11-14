#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Hiispider setup script."""

from setuptools import setup, find_packages
from hiispider import VERSION

# Also requires python-dev and python-openssl

version = '.'.join(map(str, VERSION))

setup(
    name = "HiiSpider",
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    version = version,

    install_requires = ['twisted>=8.1', 'genshi>=0.5.1', 'python-dateutil>=1.5.0,<2.0.0', 
        'ujson', 'boto', 'pyasn1', 'txredis'],
    include_package_data = True,

    # metadata for upload to PyPI
    author = "John Wehr",
    author_email = "johnwehr@hiidef.com",
    description = "HiiDef Web Services web crawler",
    license = "MIT License",
    keywords = "twisted spider crawler",
    url = "http://github.com/hiidef/hiispider",
    test_suite="tests",

)
