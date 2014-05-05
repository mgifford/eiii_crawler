"""
Given a (cached) URL, print its contents to console or write to a file.
"""

import zlib
import os, sys
import hashlib
import TingtunUtils.simpleargs as simpleargs

def get_content(url, storedir="~/.eiii/crawler/store", filename=None):
    """ Get then write or print content """

    urlhash = hashlib.md5(url).hexdigest()
    # Assumes standard store dir
    # First two bytes for folder, next two for file
    folder, sub_folder, fname = urlhash[:2], urlhash[2:4], urlhash[4:]
    # Folder is inside 'store' directory
    dirpath = os.path.expanduser(os.path.join(storedir, folder, sub_folder))
    # Data file
    fpath = os.path.expanduser(os.path.join(dirpath, fname))
    if os.path.isfile(fpath):
        data = zlib.decompress(open(fpath).read())
        if filename:
            print 'Writing to',filename,'...'
            open(filename,'w').write(data)
        else:
            print data
    else:
        print 'No cached data for',url

if __name__ == "__main__":
    options = simpleargs.parse({"-u": ("url","URL to check for"),
                                "-f?": ("filename", "Write to filename"),
                                "-s?": ("storedir", "Store directory")},
                               storedir="~/.eiii/crawler/store")

    get_content(options.url, options.storedir, options.filename)
