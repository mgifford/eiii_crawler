# -- coding: utf-8

""" Module containing methods and classes to download URLs, parse HTML
content and find child URLs etc """

import requests
import urllib2
import urllib
import httplib
import socket
import contextlib
import sgmllib
import urlparse
import base64
import zlib

___author__ = "Anand B Pillai"
__maintainer__ = "Anand B Pillai"
__version__ = "0.1"

# List of TLD (top-level domain) name endings from http://data.iana.org/TLD/tlds-alpha-by-domain.txt
# courtesy HarvestMan, stored in base64 encoded, compressed, string form
__tldstring__ = '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\n'

__tlds__ = zlib.decompress(base64.decodestring(__tldstring__)).split('.')

class FetchUrlException(Exception):
    def __init__(self, message):
        self.message = message
        self.origclass = message.__class__

    def __str__(self):
        return "FetcherException (wrapping: %s): %s" % (self.origclass.__name__,
                                                        self.message)

@contextlib.contextmanager
def fetch(url, *exceptions, **headers):
    
    try:
        # Don't bother verifying SSL certificates for HTTPS urls.
        yield requests.get(url, headers=headers, verify=False)
        # Catch a bunch of network errors - courtesy havestman
    except exceptions, e:
        raise FetchUrlException(e)
    except Exception, e:
        raise

@contextlib.contextmanager
def fetch_ftp(url, *exception, **headers):
    """ Fetch ftp urls using urlgrabber library """
    
    try:
        f = urllib2.urlopen(url)
        # Add a new attribute called headers
        f.headers = dict(f.headers)
        # Create a new attribute called content
        f.content = f.read()
        yield f
    except (urllib2.URLError, IOError), e:
        raise FetchUrlException(e)
    except Exception, e:
        raise   
    
def fetch_url(url, headers={}):
    """ Download a URL and return a two tuple of its content and headers """

    exceptions = [requests.exceptions.RequestException,
                  urllib2.HTTPError,urllib2.URLError,
                  httplib.BadStatusLine,IOError,TypeError,
                  ValueError, AssertionError,
                  socket.error, socket.timeout]

    if url.startswith('ftp://'):
        method = fetch_ftp
    else:
        method = fetch
        
    with method(url, *exceptions, **headers) as freq:
        return (freq.content, dict(freq.headers))

    return (None, None)

def get_url(url):
    """ Download a URL and return the requests object back """
    
    exceptions = [requests.exceptions.RequestException,
                  urllib2.HTTPError,urllib2.URLError,
                  httplib.BadStatusLine,IOError,TypeError,
                  ValueError, AssertionError,
                  socket.error, socket.timeout]

    if url.startswith('ftp://'):
        method = fetch_ftp
    else:
        method = fetch
        
    with method(url, *exceptions) as freq:
        return freq

def get_root_website(site):
    """ Get the root website. For example this returns
    foo.com if the input is images.foo.com or static.foo.com
    i.e <anything>.foo.com """

    # Code courtesy HarvestMan web crawler.
    if site.count('.') > 1:
        dotstrings = site.split('.')
        # now the list is of the form => [vodka, bar, foo, com]

        # Skip the list for skipping over tld domain name endings
        # such as .org.uk, .mobi.uk etc. For example, if the
        # server is games.mobileworld.mobi.uk, then we
        # need to return mobileworld.mobi.uk, not mobi.uk
        dotstrings.reverse()
        idx = 0

        for item in dotstrings:
            if item.lower() in __tlds__:
                idx += 1

        return '.'.join(dotstrings[idx::-1])
    else:
        # The server is of the form foo.com or just "foo"
        # so return it straight away
        return site
    
    
def get_website(url, scheme=False):
    """ Given the URL, return the site """

    # No scheme in front, add scheme
    # Get site root
    urlp = urlparse.urlparse(url)
    if urlp.netloc=='':
        # Missing scheme, add it
        url = 'http://' + url
        urlp = urlparse.urlparse(url)

    if scheme:
        # => http://www.foo.com
        return urlp.scheme + '://' + urlp.netloc
    else:
        # => www.foo.com
        return urlp.netloc

def get_full_url(url):
    """ Prefix HTTP scheme in front of URL
    if not present already """

    urlp = urlparse.urlparse(url)
    if urlp.netloc=='':
        # Missing scheme, add it
        url = 'http://' + url

    return url

class URLLister(sgmllib.SGMLParser):
    """ Simple HTML parser using sgmllib's SGMLParser """

    def reset(self):                             
        sgmllib.SGMLParser.reset(self)
        self.urls = []
        self.urldict = {}
        self.lasthref = ''

    def finish_starttag(self, tag, attrs):
        try:
            method = getattr(self, 'start_' + tag)
        except AttributeError:
            try:
                method = getattr(self, 'do_' + tag)
            except AttributeError:
                self.unknown_starttag(tag, attrs)
                return -1
            else:
                self.handle_starttag(tag, method, attrs)
                return 0

        else:
            self.stack.append(tag)          
            self.handle_starttag(tag, method, attrs)
            return 1
        
    def handle_data(self, data):
        
        if len(self.stack):
            last_tag = self.stack[-1]
            if last_tag == 'a':
                for url in self.lasthref:
                    # Keep an association of detected child URL
                    # to the data.
                    self.urldict[url] = data.strip()

    def start_a(self, attrs):
        href = [v for k, v in attrs if k=='href']
        if href:
            self.urls.extend(href)
            self.lasthref = href