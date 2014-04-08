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
import urlnorm
import re

___author__ = "Anand B Pillai"
__maintainer__ = "Anand B Pillai"
__version__ = "0.1"

# Regular expression for anchor tags
anchor_re = re.compile(r'\#+')
# Regular expression for www prefixes at front of a string
www_re = re.compile(r'^www(\d*)\.')
# Regular expression for www prefixes anywhere
www2_re = re.compile(r'www(\d*)\.')

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

def get_url(url, headers={}):
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
        
    with method(url, *exceptions, **headers) as freq:
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

def guess_content_type(url):
    """ Return a valid content-type if the URL is one of supported
    file types. Guess from the extension if any. If no extension
    found assume text/html """

    # If this is a type we don't process this will
    # return None
    urlp = urlparse.urlparse(url)
    path = urlp.path

    if path:
        splitpath = urlp.path.rsplit('.', 1)
        if len(splitpath)==2:
            ext = splitpath[-1].lower()

            # We support PDF, word and ODF
            if ext == 'pdf':
                return 'application/pdf'
            elif ext in ('doc', 'rtf'):
                return 'application/msword'
            elif ext in ('.ppt'):
                return 'application/vnd.ms-powerpoint'
            elif ext in ('docx',):
                return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            elif ext in ('odt',):
                return 'application/vnd.oasis.opendocument.text'
            elif ext in ('odp',):
                return 'application/vnd.oasis.opendocument.presentation'
            elif ext in ('ods',):
                return 'application/vnd.oasis.opendocument.spreadsheet'
            else:
                return 'text/html'
        else:
            # No extension - assume HTML
            return 'text/html'
    else:
        # No path, assume HTML
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
        return guess_content_type(url)
        

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

class URLBuilder(object):
    """ URL builder for building complete child URLs using
    URL bits and source URLs """
    
    def __init__(self, url, parent_url=''):
        self.url = url
        self.parent_url = parent_url

    def normalize(self, url):
        try:
            return urlnorm.norms(url)
        except:
            return url
        
    def build(self):
        """ Build the full child URL using the original child URL
        and the parent URL (NOTE: Parts of this code has been
        borrowed from HarvestMan's urlparser library) """

        # Courtesy: HarvestMan
        url = self.url
        parent_url = self.parent_url
        
        url = url.strip()
            
        if not url:
            return ''

        # Plain anchor URLs
        if any([url.startswith(item) for item in ('#','mailto:','javascript:','tel:')]):
            return ''

        # Seriously I am surprised we don't handle anchor links properly yet.
        if '#' in url:
            items = anchor_re.split(url)
            if len(items):
                url = items[0]
            else:
                # Forget about it
                return ''
            
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
    hulist = [ URLBuilder('http://www.yahoo.com/photos/my photo.gif'),
               URLBuilder('http://www.rediff.com:80/r/r/tn2/2003/jun/25usfed.htm'),
               URLBuilder('http://cwc2003.rediffblogs.com'),
               URLBuilder('/sports/2003/jun/25beck1.htm',
                          'http://www.rediff.com'),
               URLBuilder('http://ftp.gnu.org/pub/lpf.README'),
               URLBuilder('http://www.python.org/doc/2.3b2/'),
               URLBuilder('//images.sourceforge.net/div.png', # Works
                          'http://sourceforge.net'),
               URLBuilder('http://pyro.sourceforge.net/manual/LICENSE'),
               URLBuilder('python/test.htm', 
                          'http://www.foo.com/bar/index.html'),
               URLBuilder('/python/test.css',
                          'http://www.foo.com/bar/vodka/test.htm'),
               URLBuilder('/visuals/standard.css', 
                          'http://www.garshol.priv.no/download/text/perl.html'),
               URLBuilder('www.fnorb.org/index.html', # Works
                          'http://pyro.sourceforge.net'), 
               URLBuilder('http://profigure.sourceforge.net/index.html',
                          'http://pyro.sourceforge.net'),
               URLBuilder('#anchor',   # Hmmm, 
                          'http://www.foo.com/bar/index.html'),
               URLBuilder('nltk_lite.contrib.fst.draw_graph.GraphEdgeWidget-class.html#__init__#index-after', # O.K
                          'http://nltk.sourceforge.net/lite/doc/api/term-index.html'),
               URLBuilder('../../icons/up.png',  # Works
                          'http://www.python.org/doc/current/tut/node2.html'),
               URLBuilder('../../eway/library/getmessage.asp?objectid=27015&moduleid=160', # Works
                          'http://www.eidsvoll.kommune.no/eway/library/getmessage.asp?objectid=27015&moduleid=160'),
               URLBuilder('fileadmin/dz.gov.si/templates/../../../index.php', # Works
                          'http://www.dz-rs.si'),
               URLBuilder('http://www.evvs.dk/index.php?cPath=26&osCsid=90207c4908a98db6503c0381b6b7aa70',
                          'http://www.evvs.dk'),
               URLBuilder('http://arstechnica.com/reviews/os/macosx-10.4.ars'),
               URLBuilder('../index.php','http://www.foo.com/bar/'),
               URLBuilder('./index.php','http://www.yahoo.com/images/public/'),
               URLBuilder('http://www.foo.com/foo/../index.php')]    

    for item in hulist:
        print item.build()
