#!/usr/bin/env python
# coding: utf-8

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name = "eiii-crawler",
      version = "0.01",
      description = "A crawler developed for the EIII project.",
      keywords = "Web Crawler",
      author = "Anand B Pillai",
      author_email = "abp@tingtun.no",
      url = "http://github.com/tingtun/eiii-crawler",
      install_requires = ['pyparsing>=2.0.1', 'requests>=2.0.1'],
      license = "BSD3",
      long_description = """""",
      packages = ['eiii_crawler','js'],
      scripts = ['eiii_crawler/eiii_crawler_server.py', 'eiii_crawler/eiii_crawler_server']
      )
