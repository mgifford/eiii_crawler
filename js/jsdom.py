# -- coding: utf-8
"""
A module which provides classes that mimic the basic
Javascript DOM objects.

"""

"""
Original Copyright.

jsdom.py - Defines classes for Javascript DOM objects.
This module is part of the HarvestMan program.For licensing
information see the file LICENSE.txt that is included in this
distribution.

Created Anand B Pillai <abpillai at gmail dot com> Oct 2 2007

Copyright (C) 2007 Anand B Pillai.

"""

import urlparse

class Base(object):
    """ Base class for DOM objects """

    def __init__(self):
        pass
        
class Window(Base):
    """ DOM class which mimics a browser Window """
    
    def __init__(self, parent=None):
        self.frames = []
        self.closed = False
        self.defaultStatus = ''
        self.document = Document()
        self.history = History()
        self.length = 1
        self.location = Location()
        self.name = ''
        self.opener = None 
        self.outerheight = 0
        self.outerwidth = 0 
        self.pageXOffset = 0
        self.pageYOffset = 0
        self.parent = parent
        self.status = ''
        self.top = None
        super(Window, self).__init__()

window = Window

class Location(Base):
    """ DOM class for page location """
    
    __slots__ = ['hash','host','hostname','href','pathname','port',
                 'protocol','search','hrefchanged']

    def __init__(self):
        self.hash = ''
        self.host = ''
        self.hostname = '' 
        self.href = ''
        self.pathname = ''
        self.port = 80
        self.protocol = 'http'
        self.search = ''
        # Internal flag
        self.hrefchanged = False        
        super(Location, self).__init__()
        # Calculate
        self.__calculate()

    def replace(self, url):
        self.href =  url
        self.hrefchanged = True

    def assign(self, url):
        self.replace(url)

    def __calculate(self):
        """ Calculate all internal properties """

        p = urlparse.urlparse(self.href)
        self.hash = p.fragment
        host = p.netloc
        # Protocol
        self.protocol = p.scheme
        # Host with port
        if ':' in host:
            self.host = host
            # Host without port
            self.hostname = host[:host.rfind(':')]
            self.port = int(host[host.rfind(':'):])
        else:
            # Default port 80
            self.host = host + ':80'
            self.port = 80
            self.hostname = host

        # Origin
        self.origin = self.protocol + '://' + self.host
        self.pathname = p.path
        self.search = p.query
                 
location = Location

class Document(Base):
    """ DOM class for the document """
    
    def __init__(self):
        super(Document, self).__init__()
        self.body = ''
        self.cookie = ''
        self.domain = ''
        self.lastModified = ''
        self.referer = ''
        self.title = ''
        self.URL = ''
        self.content = ''
        self.domcontent = ''
        # Text before <script...> tags
        self.prescript = ''
        # Text after </script>..
        self.postscript = ''
        # Internal flag
        self.contentchanged = False
        
    def chomp(self, start, end):
        """ Split content according to start and end of javascript tags """

        # All content before <script...>
        self.prescript = self.content[:start]
        # All content after </script>        
        self.postscript = self.content[end:]
        
    def write(self, text):
        # Called for document.write(...) actions
        self.domcontent = self.domcontent + text

    def writeln(self, text):
        # Called for document.writeln(...) actions        
        self.domcontent = self.domcontent + text + '\n'

    def construct(self):
        """ Reconstruct document content using modified DOM """
        
        self.contentchanged = True
        self.content = ''.join((self.prescript, self.domcontent, self.postscript))

    def __repr__(self):
        return self.content

document = Document

class History(object):
    """ Javascript history object """

    def __init__(self):
        self.length = 0
        self._idx = 0
        self.pages = []

    def back(self):
        """ Go back one page in history """
        if self._idx > 0:
            self._idx -= 1

        return self.pages[self._idx]

    def forward(self):
        """ Go forward one page in history """
        if self._idx < len(self.pages) - 1:
            self._idx += 1

        return self.pages[self._idx]

    def go(self, index):
        """ Load a specific URL from history list """

        # index can be negative
        newidx = self._idx + index

        if newidx >= 0 or newidx < len(self.pages) - 1:
            self._idx = newidx
            return self.pages[newidx]
        else:
            # Wrong index request
            return None
        
history = History
