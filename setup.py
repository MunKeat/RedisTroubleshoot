#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup
from setuptools import find_packages

SOURCE_DIRECTORY = "src"

setup(name="redis-troubleshooting", version="0.1",
      package_dir={'': SOURCE_DIRECTORY},
      packages=find_packages(where=SOURCE_DIRECTORY))
