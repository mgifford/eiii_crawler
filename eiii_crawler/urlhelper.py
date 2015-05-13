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
import mimetypes
import re

from eiii_crawler import urlnorm
import eiii_crawler.utils as utils
from bs4 import BeautifulSoup

# Filter warnings by requests library on certificate issue.
# import warnings
# warnings.filterwarnings("ignore") # NOTE: This is BAD, so it is a workaround to be worked around
                                  # later once we add certficate verification.


___author__ = "Anand B Pillai"
__maintainer__ = "Anand B Pillai"
__version__ = "0.1"

# Regular expression for anchor tags
anchor_re = re.compile(r'\#+')
# Regular expression for www prefixes at front of a string
www_re = re.compile(r'^www(\d*)\.')
# Regular expression for www prefixes anywhere
www2_re = re.compile(r'www(\d*)\.')
# Page titles
title_re = re.compile('\<title\>(.*?)\<\/title\>',re.UNICODE)
# Match HTML entities of the form &#xyz; 
entity_re = re.compile(r'\&#[a-zA-Z0-9]+\;')
# Match quoted entities of the form %0D
quoted_re=re.compile('\%\d[a-zA-Z]')

# List of TLD (top-level domain) name endings from http://data.iana.org/TLD/tlds-alpha-by-domain.txt
# courtesy HarvestMan, stored in base64 encoded, compressed, string form
__tldstring__ = 'eJxFVgly6yoQPNHvsh3HiY/DJoTFIrNYlk7/GynJq2oNMzAMMJstFIQS2oSVY00ZQ\
kMYIieIAcIS\nJiquOggPESAiBNeeEFTOauR8ngVEIRzHCtEgFog3QXmDFETunxUuFkgJqSENkb2LHEqFH\
CAt5Ajp\nCC67iQqOux+QXqiJtHEmQHJHIlp1zz5Dy7RJC815fVCTOdM2bn5BLpAr5AYlCMkvmNzZMJNkX\
To1\nfYfi3fmZ7KKFUlAaykTKHBLUAMXpkTCCW8fsSg18tnLg7ZQnjIj7Xp/qeDCNBwaoCEULSRuqp2Ew\n\
hoNPNvYx9C+06OrauVnEY2z7ySnq1DfFUnNT1aW4C83X/YAUa94D13XSdMwl3wnvmBqXaY12cnOl\nn96gX\
lAL1BuMqtqgRVSGdLenyZmQVKYrtBOBpxcy2fQjVugH9AQdoOMMnYiwR1RvMApGt/7Rhf2W\nfKOxMFTw2L0\
45/0G5tncHDgDPq/LpdLnMJS5/cUFzr1nk2sfUjEag8gBgyPKuJN+1eGBYcLgnR25\nY/CpxwMDFRPRoj6uM\
WQMrUq6xArCe8N3WAmrYQ3sAGthR1hHDBWWel6UAhtgI2x6wc6wT9hMiHl0\nimsEVRvhqqDrbcsUFtgVo2g\
F44QxYIwYk9e8bOmM04LLjJQhbZ1mjBVjA9PdGdBPLhAhSeediXAd\nujHwjn5ykQ9zceJXqqvMDnK1fybH/a\
2sT0eVJ1yG44aKhyHqRibgkQhWymPGJNpA+yywyWIaMTki\n8KvMbc67hROUySbjSbklEwzFtGBaMW3wgogaX\
sIreIc9ED0w3oVEwpt6oudvhM/wBb7CN+LdGAP/\ngl8RBOEKVYKIbDV7YpC1iTRPZrcYFIJGMATThCoNgbMj\
AlPUkXqECYE0INAS99KHJJrWE7fRN8wc\nFnx4ImSEgkA7tMIwNO55ISwIb7ALhg1REDatfeChUYF1GnlyNAw\
GO1kcwFtFR8QHtTxiAisicqkh\nrtywoRejWARY34kaKVuwTc4i19ibTWdIDeYBM5dGItV00D3T1oOnUs+5uT\
t0JjzRgux+mQPmCNZI\nxZyJ1D99dInS+V5FPXdmgjpsRvOCecVT4DnT59kQys1U6cHNpk/FKnyXZ8E84pBYi\
bm36Gxezixc\ncuyCPIxnsS/yLUz+3JjNeUERhMh7ahSJolDIGOK9olgwDCzfqYzUGxPPLY6I1nf2gTKheJSA\
ElES\noRyzuiQWSUmeridtx/MKhYrChTbPvr9yZ9Z96HcuL5Q3CifWUg1jXjaw1dSUUBWqRjVqjL0Nr2Q9\n6\
oBqUUfQZeweHOi2+kCdULkaUCMYoJp6Hdc0rZ1PdFWdUTPBn9FO3f4bQObVjVbUF+qCuqEJNIs2\noTFd2CXa\
irbhJYijZxa8FF6GsPxd6a2w0fF4WbwcIR5dcN73RVd6d3sRSU80kdgPSPrRr55HL+Yv\nG9yrYWE1kbC2sRj\
J3z0OGsuApTeZxbH6l5SnAob2Hf/770OWejqFg9flev2wB29O8uv0ZXbh+inz\n4652/vPzuVwv9pd/f+qdvQ1\
5+/6Zvj2f9++b/Hj7Xfw+iXQ5i1++mFHLP2E5ttxP4kMcB6izeB2T\nyl4l77uzXo3qZMRJXuxF3K06TlVbbpe\
D1WehHh+H4cE9L5dv9TmWX/F2/Vsp33+z2/2HnVV/38dh\nadjURd3N5biONerv7ePlnx/cWd7kWdzE5fCRSyfx\
dVz2cRZsVzt3W873H49Oc17OP0eQXz+/Dt7z\n4odJb6UU9Sz092OXg5V3sfC/0o9AF12H8038ySJ8ie/xT5QXq\
f+Edfyy85+qHM/i62x+RXUSd7Ep\n+yvz9/oqPvWVNfcz874qfRKH0yI3fIrtsB2fr6/hH1dOJ/7q73L6UMv1uE\
2ych6+hyP+81kcfpmt\nLCd9aDzvj1Vej3fn8fm6347LlPs/F7fY5uu6s4vlC9Qv63988J7URfiPcTWXfxPaf4hP\
Y07HOeuQ\n09XdvtIhWekuIrD5d2kb8nlPjDfldcNqsFZsAlvAlvgzsC3/A+nsAxc=\n'

__tlds__ = zlib.decompress(base64.decodestring(__tldstring__)).split('.')

# Add all tlds which dont evaluate to text/html
__mimetypes__ = {tld: 'text/html' for tld in filter(lambda x: mimetypes.guess_type('http://foo.'+x)[0],
                                                    __tlds__)}
__mimetypes__['gz'] = 'application/x-gzip'

class FetchUrlException(Exception):
    def __init__(self, message):
        self.message = message
        self.origclass = message.__class__

    def __str__(self):
        return "FetcherException (wrapping: %s): %s" % (self.origclass.__name__,
                                                        self.message)

class InvalidContentType(Exception):
    pass

class MaxRequestSizeExceeded(Exception):
    pass

@contextlib.contextmanager
def fetch(url, *exceptions, **headers):
    
    try:
        # If a proxy is specified, set it.
        proxy = headers.get('proxy')
        # SSL cert verify
        verify = headers.get('verify', False)

        if proxy:
            proxies = {'http' : proxy, 'https' : proxy}
            # Add a timeout of 15s
            yield requests.get(url, headers=headers, proxies=proxies, verify=verify, timeout=15, stream=True)
        else:
            yield requests.get(url, headers=headers, verify=verify, timeout=15, stream=True)          
        # Catch a bunch of network errors - courtesy havestman
    except exceptions, e:
        raise FetchUrlException(e)
    except Exception, e:
        raise

@contextlib.contextmanager
def fetch_quick(url, *exceptions, **headers):
    """ Fetch a URL immediately """
    
    try:
        # SSL cert verify
        verify = headers.get('verify', False)       
        # If a proxy is specified, set it.
        proxy = headers.get('proxy')
        if proxy:
            proxies = {'http' : proxy, 'https' : proxy}
            # Add a timeout of 15s
            yield requests.get(url, headers=headers, proxies=proxies, verify=verify, timeout=15)
        else:
            yield requests.get(url, headers=headers, verify=verify, timeout=15)          
        # Catch a bunch of network errors - courtesy havestman
    except exceptions, e:
        raise FetchUrlException(e)
    except Exception, e:
        raise   

@contextlib.contextmanager
def head(url, *exceptions, **headers):
    
    try:
        # SSL cert verify
        verify = headers.get('verify', False)               
        yield requests.head(url, headers=headers, allow_redirects=True, timeout=15,
                            verify=verify)
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

def fetch_url(url, headers={}, proxy='', verify=False):
    """ Download a URL immediately """

    exceptions = [requests.exceptions.RequestException,
                  requests.exceptions.ConnectionError,
                  urllib2.HTTPError,urllib2.URLError,
                  httplib.BadStatusLine,IOError,TypeError,
                  ValueError, AssertionError,
                  socket.error, socket.timeout]

    if url.startswith('ftp://'):
        method = fetch_ftp
    else:
        method = fetch_quick

    # If proxy is set, set it in header
    if proxy: headers['proxy'] = proxy
    headers['verify'] = verify
    
    with method(url, *exceptions, **headers) as freq:
        return freq
    
def get_url(url, headers={}, proxy='', content_types=[], max_size=0, verify=False):
    """ Download a URL and return the requests object back """
    
    exceptions = [requests.exceptions.RequestException,
                  requests.exceptions.ConnectionError,                
                  urllib2.HTTPError,urllib2.URLError,
                  httplib.BadStatusLine,IOError,TypeError,
                  ValueError, AssertionError,
                  socket.error, socket.timeout]

    if url.startswith('ftp://'):
        method = fetch_ftp
    else:
        method = fetch

    # If proxy is set, set it in header
    if proxy: headers['proxy'] = proxy
    headers['verify'] = verify
    
    with method(url, *exceptions, **headers) as freq:
        # Check the content-type
        hdr = freq.headers
        ctype = hdr.get('content-type','text/html').split(';')[0]
        if len(content_types) and ctype not in content_types:
            raise InvalidContentType, 'content-type ' + ctype + ' is not valid.'
        csize = int(hdr.get('content-length', 0))
        if csize and max_size and (csize > max_size):
            raise MaxRequestSizeExceeded, "size of request %d exceeds maximum request size %d" % (csize, max_size)
        
        return freq

def head_url(url, headers={}, verify=False):
    """ Download a URL with a HEAD request and return the requests object back """
    
    exceptions = [requests.exceptions.RequestException,
                  requests.exceptions.ConnectionError,                
                  urllib2.HTTPError,urllib2.URLError,
                  httplib.BadStatusLine,IOError,TypeError,
                  ValueError, AssertionError,
                  socket.error, socket.timeout]

    method = head
    headers['verify'] = verify
    
    with method(url, *exceptions, **headers) as freq:
        return freq 

def get_url_parent_directory(url):
    """ Return the parent 'directory' of a given URL

    >>> get_url_parent_directory('http://www.foo.com')
    ''
    >>> get_url_parent_directory('http://www.foo.com/')
    ''
    >>> get_url_parent_directory('http://www.foo.com/a')
    ''
    >>> get_url_parent_directory('http://www.foo.com/a/')
    ''
    >>> get_url_parent_directory('http://www.foo.com/a/b/')
    'http://www.foo.com/a'
    >>> get_url_parent_directory('http://www.foo.com/a/b/c')
    'http://www.foo.com/a/b'
    >>> get_url_parent_directory('http://www.foo.com/a/b/c/index.html')
    'http://www.foo.com/a/b/c'
    >>> get_url_parent_directory('http://www.foo.com/a/b/c/test.css')
    'http://www.foo.com/a/b/c'
    >>> get_url_parent_directory('http://faq.tingtun.no/faq_search/index.php?query=rss&search=%26%23160%3B%26%23160%3B%26%23160%3BS%26%23248%3Bk%26%23160%3B%26%23160%3B%26%23160%3B&site=ec.europa.eu')
    'http://faq.tingtun.no/faq_search'
    """

    # If this is not HTML, skip it
    urlp = urlparse.urlparse(url)

    # If URL has params or query or fragment, return ''
    path = urlp.path
    if not path: return ''
    
    paths = [item for item in path.split('/') if item]
    if len(paths)>=2:
        paths = '/' + '/'.join(paths[:-1])
        newurl = urlparse.urlunparse(urlparse.ParseResult(scheme=urlp.scheme,
                                                          netloc=urlp.netloc,
                                                          path=paths,
                                                          fragment='',
                                                          query='',
                                                          params=''))

        return newurl

    return ''

def get_url_directory(url):
    """ Return the URL 'directory' of a given URL

    >>> get_url_directory('http://www.python.org')
    'http://www.python.org'
    >>> get_url_directory('http://www.python.org/')
    'http://www.python.org/'
    >>> get_url_directory('http://www.python.org/doc')
    'http://www.python.org/doc/'
    >>> get_url_directory('http://www.python.org/doc/')
    'http://www.python.org/doc/'
    >>> get_url_directory('http://www.python.org/doc/index.html')
    'http://www.python.org/doc/'
    >>> get_url_directory('http://docs.python.org/doc/index.html')
    'http://docs.python.org/doc/'
    >>> get_url_directory('http://faq.tingtun.no/faq_search/index.php?query=rss&search=%26%23160%3B%26%23160%3B%26%23160%3BS%26%23248%3Bk%26%23160%3B%26%23160%3B%26%23160%3B&site=ec.europa.eu')
    'http://faq.tingtun.no/faq_search/'
    """

    # If the site does not have a path e.g: http://www.python.org
    # or http://www.python.org/, the directory is same as the URL.
    # If the site has a path e.g: http://www.python.org/docs or
    # http://www.python.org/docs/ or http://python.org/docs/file.html
    # the directory is http://www.python.org/docs/

    urlp = urlparse.urlparse(url)
    if urlp.path in ('', '/'):
        # First case
        return url

    paths = urlp.path.split('/')
    # If lastpath does not have an extension like file.html
    # then the whole thing can be assumed as a directory.
    # E.g: http://www.python.org/docs or http://www.python.org/docs/
    try:
        if paths[-1].find('.') == -1:
            # Append '/' at end if not found
            if url[-1] != '/': url += '/'
            # Can return full URL again - Sans any query fragments
            urlp=urlp._replace(query='',params='',fragment='')
            # Add / at end if not found
            if urlp.path[-1] != '/':
                urlp = urlp._replace(path=urlp.path + '/')
                
            return urlparse.urlunparse(urlp)
        else:
            # URL like http://www.python.org/doc/index.html
            # Return All minus the last path i.e
            # http://www.python.org/doc/
            paths.pop()
            urlpath = '/'.join(paths)

            urlp=urlp._replace(query='',params='',fragment='')
            urlp = urlp._replace(path=urlpath)
            newurl = urlparse.urlunparse(urlp)

            # Append '/' at end if not found
            if newurl[-1] != '/': newurl += '/'

            return newurl
    except:
        return url
    
def get_depth(url):
    """ Get 'depth' of a URL with respect to its root.

    >>> get_depth('http://www.foo.com/')
    0
    >>> get_depth('http://www.foo.com/a/')
    1
    >>> get_depth('http://www.foo.com/a/b')
    2
    >>> get_depth('http://www.foo.com/a/b/f.css')
    2

    """

    urlp = urlparse.urlparse(url)
    # Omit last path if it contains a file extension
    paths = urlp.path.split('/')
    if '.' in paths[-1]:
        paths.pop()

    # Drop empty strings
    paths = [p for p in paths if p]
    # Length of path is the depth
    return len(paths)

def is_www_of(url1, url2):
    """ Return true if the domain of url1 differs
    only by a www w.r.t domain of url2 or viceverza """

    # Schemes (http, https) are ignored
    urlp1 = urlparse.urlparse(url1)
    urlp2 = urlparse.urlparse(url2)
    # print urlp1, urlp2
    
    # First compare server (netloc)
    nl1, nl2 = urlp1.netloc, urlp2.netloc
    # print nl1, nl2
    
    if len(nl2)>len(nl1):
        left = nl2.replace(nl1, '')
    elif len(nl1)>=len(nl2):
        left = nl1.replace(nl2, '')     

    return (left.lower() == 'www.' or left=='')
    
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

        # Lowercase! E.g: http://www.gig.com.qa/                
        return '.'.join(dotstrings[idx::-1]).lower()
    else:
        # The server is of the form foo.com or just "foo"
        # so return it straight away

        # Lowercase! E.g: http://www.gig.com.qa/                        
        return site.lower()
    
    
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
        website = urlp.scheme + '://' + urlp.netloc
        # Lower-case!
        return website.lower()
    else:
        # => www.foo.com
        # Lowercase !
        return urlp.netloc.lower()

def get_full_url(url):
    """ Prefix HTTP scheme in front of URL
    if not present already """

    urlp = urlparse.urlparse(url)
    if urlp.netloc=='':
        # Missing scheme, add it
        url = 'http://' + url

    return url

def guess_content_type(url):
    """ Return a valid content-type if the URL is one of supported
    file types. Guess from the extension if any. If no extension
    found assume text/html """

    extn = url.rsplit('.')[-1]
    # Python mime-types library doesn't seem to identify .gz as
    # application/x-gzip, so adding it. This causes issue #439
    if extn in __mimetypes__:
        return __mimetypes__[extn]
    else:
        ctype, encoding = mimetypes.guess_type(url, strict=False)
        if ctype != None:
            return ctype

    # Default
    return 'text/html'

def get_content_type(url, headers):
    """ Given a URL and its headers find the content-type """

    # If there is a content-type field in the headers
    # return using that
    try:
        ctype_header = headers['content-type']
        # This can be sometimes of the form text/html; charset=utf-8, we only
        # want the former part.
        return ctype_header.split(';')[0].strip()
    except KeyError:
        p = urlparse.urlparse(url)
        # If no path, append '/' at end otherwise
        # guess_content_type reports junk results
        if p.path=='':
            url = url + '/'
            
        return guess_content_type(url)


def check_spurious_404(headers, content, status_code=200):
    """ Check for pages that are 404 pages wrapped
    in 200 disguise """

    # Get title
    title = ''.join(title_re.findall(content)).lower().strip()
    # Apply heuristics on title
    # E.g: 404, Page not found, not found, page does not exist etc
    if any(title.startswith(x) for x in ('page not found','not found','page does not exist',
                                         'error 404')):
        return 404

    # Safe to assume if a page title starts with 404 it is 404 ?
    # I hope so.
    if title.startswith('404 ') or title.endswith(' 404'):
        return 404

    return status_code

def check_spurious_404_title(title, status_code=200):
    """ Check for pages that are 404 pages wrapped
    in 200 disguise with the title given """

    # Apply heuristics on title
    # E.g: 404, Page not found, not found, page does not exist etc
    if any(title.startswith(x) for x in ('page not found','not found','page does not exist',
                                         'error 404')):
        return 404

    # Safe to assume if a page title starts with 404 it is 404 ?
    # I hope so.
    if title.startswith('404 ') or title.endswith(' 404'):
        return 404

    return status_code

def check_page_error(title, content):
    """ Check if page is loaded with error using the title
    and content """

    # Used by selenium crawler
    if title=='Problem loading page':
        # Error in connection
        raise FetchUrlException, title

class BeautifulLister(object):
    """ An HTML parser using BeautifulSoup """
    
    def __init__(self):
        self.reset()

    def feed(self, content):
        """ Feed parser with data """
        
        soup=BeautifulSoup(content, "lxml")
        # Text of links mapped to the link text
        # Caveat - right now this is a fallback parser so it parses only
        # anchor tags ('a') and nothing else. 
        self.urlmap = {item['href']:item.text for item in soup.findAll('a') if item.has_attr('href')}
        self.urls = self.urlmap.keys()      

    def close(self):
        # Dummy method to imitate most parsers
        pass
    
    def reset(self):
        """ Reset the state """
        
        self.urlmap = {}
        self.urls = []
        # Redirect URL if any
        self.follow_url = ''
        # Replaced source URL
        self.source_url = ''
        # Should we redirect to the follow URL ?
        self.redirect = False
        # Should we replace the source (parent) URL ?
        self.base_changed = False               

    def get_url_map(self):
        """ Return the URL map """
        return self.urlmap
    
class URLLister(sgmllib.SGMLParser):
    """ Simple HTML parser using sgmllib's SGMLParser """

    def reset(self):                             
        sgmllib.SGMLParser.reset(self)
        self.urls = []
        self.urldict = {}
        self.lasthref = ''
        # Redirect URL if any
        self.follow_url = ''
        # Replaced source URL
        self.source_url = ''
        # Should we redirect to the follow URL ?
        self.redirect = False
        # Should we replace the source (parent) URL ?
        self.base_changed = False

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

    def start_video(self, attrs):
        """ Parse the HTML5 <video> tag """

        adict = utils.CaselessDict(attrs)
        link = adict.get('src','')
        if link:
            self.urls.append(link)

    def start_audio(self, attrs):
        """ Parse the HTML5 <audio> tag """

        adict = utils.CaselessDict(attrs)
        link = adict.get('src','')
        if link:
            self.urls.append(link)

    def start_object(self, attrs):
        """ Parse the generic <object> tag """

        adict = utils.CaselessDict(attrs)
        link = adict.get('data','')
        if link:
            self.urls.append(link)

    def start_embed(self, attrs):
        """ Parse the (deprecated) <embed> tag """

        adict = utils.CaselessDict(attrs)
        link = adict.get('src','')
        if link:
            self.urls.append(link)          

    def start_param(self, attrs):
        """ Parse the <param> tag inside <object> tags """
        
        # Internet explorer uses <param> elements inside object
        # to specify the source of the media.
        # Ref: http://www.yourwebskills.com/htmlobject.php
        if len(self.stack):
            last_tag = self.stack[-1]
            if last_tag == 'object':
                adict = utils.CaselessDict(attrs)
                link = adict.get('value','')
                if link:
                    self.urls.append(link)              

    def start_iframe(self, attrs):
        """ Parse <iframe> tags """

        adict = utils.CaselessDict(attrs)
        link = adict.get('src','')
        if link:
            self.urls.append(link)        
        
    def start_meta(self, attrs):
        """ Parse meta tags """
        
        # print 'META TAG =>',attrs
        adict = utils.CaselessDict(attrs)

        if adict.get('http-equiv','').lower() == 'refresh':
            # Look for content key
            content = adict.get('content','').strip()
            # If URL is specified it woul be like
            # <meta http-equiv="refresh" content="5; url=http://example.com/">
            pieces = content.split(';')
            if len(pieces)>1:
                try:
                    items = pieces[1].lower().strip().split('=')
                    if items[0]=='url':
                        # We should follow this URL and NOT parse the
                        # current page.
                        self.follow_url = items[1]
                        self.redirect = True
                except:
                    pass

    def start_link(self, attrs):
        """ Parse 'link' tags """

        adict = utils.CaselessDict(attrs)
        # Handle 'stylesheet' links
        if adict.get('rel','').lower()=='stylesheet':
            css_url = adict.get('href')
            # print 'CSS URL=>',css_url
            self.urls.append(css_url)

    def start_base(self, attrs):
        """ Parse 'base' tags """

        adict = utils.CaselessDict(attrs)

        base_href = adict.get('href','')
        if base_href:
            # This needs to replace the source URL
            self.source_url = base_href
            self.base_changed = True

    def start_frame(self, attrs):
        """ Parse 'frame' tags """

        adict = utils.CaselessDict(attrs)

        # Add frame source URLs.
        src_url = adict.get('src','')
        if src_url:
            # Note - this can be a JS URL but we
            # add it anyway.
            self.urls.append(src_url)

    def start_area(self, attrs):
        """ Parse 'area' tags """

        adict = utils.CaselessDict(attrs)
        # These are defines areas inside image-maps
        url = adict.get('href','')
        if url:
            self.urls.append(url)           

    # Skipped tags - embed, option, object, applet etc.
    # Former 3 because they deal with embeddable URL like flash.
    # latter because it typically is used to load a Java applet class.
    
    
class CSSLister(object):
    """ Class to parse stylesheets and extract URLs """

    # Code courtesy HarvestMan (class: HarvestManCSSParser)
    # UNUSED.
    
    # Regexp to parse stylesheet imports
    importcss1 = re.compile(r'(\@import\s+\"?)(?!url)([\w.-:/]+)(\"?)', re.MULTILINE|re.LOCALE|re.UNICODE)
    importcss2 = re.compile(r'(\@import\s+url\(\"?)([\w.-:/]+)(\"?\))', re.MULTILINE|re.LOCALE|re.UNICODE)
    # Regexp to parse URLs inside CSS files
    cssurl = re.compile(r'(url\()([^\)]+)(\))', re.LOCALE|re.UNICODE)

    def __init__(self):
        # Any imported stylesheet URLs
        self.csslinks = []
        # All URLs including above
        self.links = []

    def feed(self, data):
        self._parse(data)
        
    def _parse(self, data):
        """ Parse stylesheet data and extract imported css links, if any """

        # Return is a list of imported css links.
        # This subroutine uses the specification mentioned at
        # http://www.w3.org/TR/REC-CSS2/cascade.html#at-import
        # for doing stylesheet imports.

        # This takes care of @import "style.css" and
        # @import url("style.css") and url(...) syntax.
        # Media types specified if any, are ignored.
        
        # Matches for @import "style.css"
        l1 = self.importcss1.findall(data)
        # Matches for @import url("style.css")
        l2 = self.importcss2.findall(data)
        # Matches for url(...)
        l3 = self.cssurl.findall(data)
        
        for item in (l1+l2):
            if not item: continue
            url = item[1].replace("'",'').replace('"','')
            self.csslinks.append(url)
            self.links.append(url)
            
        for item in l3:
            if not item: continue
            url = item[1].replace("'",'').replace('"','')
            if url not in self.links:
                self.links.append(url)

class URLBuilder(object):
    """ URL builder for building complete child URLs using
    URL bits and source URLs """
    
    def __init__(self, url, parent_url=''):
        self.url = url
        self.parent_url = parent_url

    def normalize(self, url):

        try:
            urln = urlnorm.norms(url)
        except:
            urln = url

        try:
            return unicode(urln, 'latin1')
        except TypeError:
            return urln
        
    def build(self):
        """ Build the full child URL using the original child URL
        and the parent URL (NOTE: Parts of this code has been
        borrowed from HarvestMan's urlparser library)

        >>> URLBuilder('http://www.yahoo.com/photos/my photo.gif').build()
        u'http://www.yahoo.com/photos/my photo.gif'
        >>> URLBuilder('http://www.rediff.com:80/r/r/tn2/2003/jun/25usfed.htm').build()
        u'http://www.rediff.com/r/r/tn2/2003/jun/25usfed.htm'
        >>> URLBuilder('http://cwc2003.rediffblogs.com').build()
        u'http://cwc2003.rediffblogs.com'
        >>> URLBuilder('/sports/2003/jun/25beck1.htm','http://www.rediff.com').build()
        u'http://www.rediff.com/sports/2003/jun/25beck1.htm'
        >>> URLBuilder('http://ftp.gnu.org/pub/lpf.README').build()
        u'http://ftp.gnu.org/pub/lpf.README'
        >>> URLBuilder('http://www.python.org/doc/2.3b2/').build()
        u'http://www.python.org/doc/2.3b2/'
        >>> URLBuilder('//images.sourceforge.net/div.png', 'http://sourceforge.net').build()
        u'http://images.sourceforge.net/div.png'
        >>> URLBuilder('http://pyro.sourceforge.net/manual/LICENSE').build()
        u'http://pyro.sourceforge.net/manual/LICENSE'
        >>> URLBuilder('python/test.htm', 'http://www.foo.com/bar/index.html').build()
        u'http://www.foo.com/bar/python/test.htm'
        >>> URLBuilder('/python/test.css','http://www.foo.com/bar/vodka/test.htm').build()
        u'http://www.foo.com/python/test.css'
        >>> URLBuilder('/visuals/standard.css', 'http://www.garshol.priv.no/download/text/perl.html').build()
        u'http://www.garshol.priv.no/visuals/standard.css'
        >>> URLBuilder('www.fnorb.org/index.html', 'http://pyro.sourceforge.net').build()
        u'http://www.fnorb.org/index.html'
        >>> URLBuilder('http://profigure.sourceforge.net/index.html','http://pyro.sourceforge.net').build()
        u'http://profigure.sourceforge.net/index.html'
        >>> URLBuilder('#anchor', 'http://www.foo.com/bar/index.html').build()
        u''
        >>> URLBuilder('nltk_lite.contrib.fst.draw_graph.GraphEdgeWidget-class.html#__init__#index-after','http://nltk.sourceforge.net/lite/doc/api/term-index.html').build()
        u'http://nltk.sourceforge.net/lite/doc/api/nltk_lite.contrib.fst.draw_graph.GraphEdgeWidget-class.html'
        >>> URLBuilder('../../icons/up.png', 'http://www.python.org/doc/current/tut/node2.html').build()
        u'http://www.python.org/doc/icons/up.png'
        >>> URLBuilder('../../eway/library/getmessage.asp?objectid=27015&moduleid=160', 'http://www.eidsvoll.kommune.no/eway/library/getmessage.asp?objectid=27015&moduleid=160').build()
        u'http://www.eidsvoll.kommune.no/eway/library/getmessage.asp?objectid=27015&moduleid=160'
        >>> URLBuilder('fileadmin/dz.gov.si/templates/../../../index.php', 'http://www.dz-rs.si').build()
        u'http://www.dz-rs.si/index.php'
        >>> URLBuilder('http://www.evvs.dk/index.php?cPath=26&osCsid=90207c4908a98db6503c0381b6b7aa70','http://www.evvs.dk').build()
        u'http://www.evvs.dk/index.php?cPath=26&osCsid=90207c4908a98db6503c0381b6b7aa70'
        >>> URLBuilder('http://arstechnica.com/reviews/os/macosx-10.4.ars').build()
        u'http://arstechnica.com/reviews/os/macosx-10.4.ars'
        >>> URLBuilder('../index.php','http://www.foo.com/bar/').build()
        u'http://www.foo.com/index.php'
        >>> URLBuilder('./index.php','http://www.yahoo.com/images/public/').build()
        u'http://www.yahoo.com/images/public/index.php'
        >>> URLBuilder('http://www.foo.com/foo/../index.php').build()
        u'http://www.foo.com/index.php'
        >>> 

        """

        # Courtesy: HarvestMan
        url = self.url
        parent_url = self.parent_url
        
        url = url.strip()
            
        if not url:
            return u''

        # Plain anchor URLs or other types of 'junk' URLs
        if any([url.startswith(item) for item in ('#','mailto:','javascript:','tel:', 'file:')]):
            return u''

        # If the URL consists of HTML entities, then convert them to their proper form.
        # E.g: &#xD;&#xA;/English/News/Global/pages/Singapore-smog-eases-as-Indonesian-planes-water-bomb-fires.aspx
        # from http://www.moe.gov.qa/English/SitePages/Default.aspx
        if entity_re.search(url):
            url = utils.unescape(url)

        # This unquoting is probably unnecessary and caused issue #457
        # The problem is unless string casted, it returns a unicode string
        # which actually contains byte characters which then can't be further
        # encoded or decoded in proper ascii or unicode.
        #if quoted_re.search(url):
        #    url = urllib.unquote(url)

        # Do same for parent URL
        if entity_re.search(parent_url):
            parent_url = utils.unescape(parent_url)

        # This unquoting is probably unnecessary and caused issue #457
        #if quoted_re.search(parent_url):
        #    parent_url = urllib.unquote(parent_url)
            
        # Strip again as unescape or unquote could have introduced newline characters.
        url = url.strip()
        if not url:
            return u''
        
        parent_url = parent_url.strip()
        
        # If the URL has many quoted characters, unquote it.
        # Seriously I am surprised we don't handle anchor links properly yet.
        if '#' in url:
            items = anchor_re.split(url)
            if len(items):
                url = items[0]
            else:
                # Forget about it
                return u''
            
        if (url.startswith('http:') or url.startswith('https:')):
            return self.normalize(url)

        # What about FTP ?
        if url.startswith('ftp:'):
            return self.normalize(url)

        # We accept FTP urls beginning with just
        # ftp.<server>, and consider it as FTP over HTTP
        if url.startswith('ftp.'):
            # FTP over HTTP
            url = 'http://' + url           
            return self.normalize(url)            

        # URLs not starting with https?: or ftp: but with www, i.e http
        # omitted - we need to treat them as HTTP.
        if www_re.match(url):
            url = 'http://' + url
            return self.normalize(url)          
            
        # Urls relative to server might begin with a //. Then prefix the
        # protocol string to them.
        if url.startswith('//'):
            if parent_url:
                protocol = urlparse.urlparse(parent_url).scheme  + '://'
            else:
                protocol = "http://"   

            url = protocol + url[2:]
            return self.normalize(url)                      
            
        protocol,domain,path,dummy,dummy,dummy = urlparse.urlparse(parent_url)
        
        if url.startswith('/'):
            try:
                url = protocol + '://' + domain + url
            except UnicodeDecodeError:
                # Quote URL
                url = protocol + '://' + domain + urllib.quote(url)
        else:
            path = path[:path.rfind('/')]
            try:
                url = protocol +'://' + domain + '/' + path +'/' + url
            except UnicodeDecodeError:
                # Quote URL
                url = protocol +'://' + domain + '/' + path +'/' + urllib.quote(url)

        return self.normalize(url)

if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)
