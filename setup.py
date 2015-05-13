#!/usr/bin/env python
# coding: utf-8
import shutil

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name = "eiii-crawler",
      version = "0.9",
      description = "A crawler developed for the EIII project.",
      keywords = "Web Crawler",
      author = "Anand B Pillai",
      author_email = "abp@tingtun.no",
      url = "http://github.com/tingtun/eiii-crawler",
      install_requires = ['pyparsing>=2.0.1', 'requests>=2.0.1, !=2.4.0'],
      license = "BSD3",
      long_description = """A modular, pluggable, comprehensive web-crawler developed for the EIII project""",
      packages = ['eiii_crawler','js','eiii_crawler.plugins'],
      scripts = ['eiii_crawler/eiii_crawler_server.py', 'eiii_crawler/eiii_crawler_server']
      )

# Copy eiii_crawler_serverd to /etc/init.d/
print 'Copying init.d script ...'
shutil.copy('eiii_crawler/scripts/eiii_crawler_serverd', '/etc/init.d/')
