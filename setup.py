#!/usr/bin/env python
# coding: utf-8

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name = "eiii-crawler",
      version = "0.9",
      description = "A crawler developed for the EIII project.",
      keywords = "Web Crawler",
      author = "Various",
      author_email = "contact@tingtun.no",
      url = "http://gitlab.tingtun.no/eiii_source/eiii_crawler",
      install_requires = [
          'pyparsing>=2.0.1',
          'requests>=2.0.1, !=2.4.0',
          'pyparsing>=2.0.1',
          'requests>=2.0.1',
          'BeautifulSoup4>=4.3.2',
          'lxml>=3.3.5',
          'sgmlop>=1.1.1'
          ],
      license = "BSD3",
      long_description = """A modular, pluggable, comprehensive web-crawler developed for the EIII project""",
      packages = ['eiii_crawler','js','eiii_crawler.plugins'],
      scripts = ['eiii_crawler/eiii_crawler_server.py', 'eiii_crawler/eiii_crawler_server']
      )
