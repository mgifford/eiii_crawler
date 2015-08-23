#!/usr/bin/env python

""" Clean crawler cache at ~/.eiii folder periodically """

import os
import sys
import time
import datetime

CACHE_ROOT=os.path.expanduser('~/.eiii/crawler/')

def files_to_clean(limit=7):
    """ Return files which are older than <limit> days as a generator """

    for dirf in ('store','stats'):
        cache_f = os.path.join(CACHE_ROOT, dirf)
        now = datetime.datetime.now()
    
        for root, files, dirs in os.walk(cache_f):
            for fpath in files:
                fullfpath = os.path.join(root, fpath)
                time_m = datetime.datetime.fromtimestamp(os.stat(fullfpath).st_mtime)
                delta = now - time_m
                # print fullfpath, delta.days
                if delta.days > limit:
                    yield fullfpath

def clean_files(limit=7):
    """ Clean up files older than <limit> days from crawler cache """

    for fpath in files_to_clean(limit=limit):
        try:
            print 'Cleaning',fpath,'...'
            os.remove(fpath)
        except Exception, e:
            print '\t',e
            pass
    
if __name__ == "__main__":
    clean_files()
