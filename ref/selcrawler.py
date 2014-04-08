"""

Prototypical selenium based web crawler

"""

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait # available since 2.4.0
from selenium.webdriver.support import expected_conditions as EC # available since 2.26.0

import sgmllib
import urlparse
import urllib
import re
import socket
import collections
import time
import hashlib

from contextlib import contextmanager

msword_re = re.compile(r'microsoft\s+(word|powerpoint|excel)\s*-', re.IGNORECASE)
paren_re = r=re.compile('^\(|\)$')
# Regular expression for anchor tags
anchor_re = re.compile(r'\#+')

socket.setdefaulttimeout(120)

___author__ = "Anand B Pillai"
__maintainer__ = "Anand B Pillai"
__version__ = "0.01 alpha"
__lastmodified__ = "Thu Mar 27 00:03:15 IST 2014"

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

class CrawlerConfig(object):
    """ Crawler configuration """

    def __init__(self, maxdepth=10, maxurls=100, strict=True):
        self.maxdepth = maxdepth
        self.maxurls = maxurls
        self.strict = strict
    
class SeleniumCrawler(object):
    """ Web-crawler based on selenium """

    def __init__(self, url, strict=True, use_params=False, maxlevel=10, maxurls=100):
        """ Crawl from starting URL 'url' upto 'n' levels """
        
        self.url = url
        self.maxlevel = maxlevel
        self.maxurls = maxurls
        
        self.nurls = 0
        # List containing 2 tuples of (url, data)
        self._content = {}
        self.strict = strict
        # All URLs
        self.url_levels = collections.defaultdict(int)
        self.allurls = {}
        self.flag = False
        if not use_params:
            # Make base URL comparison without URL params
            p = urlparse.urlparse(url)._replace(params='',query='')
            self.url_nop = p.geturl()
        else:
            self.url_nop = url
        # Create a new instance of the Firefox driver
        self.driver = webdriver.Firefox()           

    def makeProperURL(self, sourceurl, url):
        """ Given a parent URL and a child URL, return the fully
        formed and normalized child URL """

        url = url.strip()
            
        if not url:
            return ''

        # Plain anchor URLs
        if url.startswith('#'):
            return ''
        if url.startswith('mailto:'):
            return ''
        if url.startswith('javascript:'):
            return ''
        if url.startswith('tel:'):
            return ''

        # Seriously I am surprised we don't handle anchor links
        # properly yet.
        if '#' in url:
            items = anchor_re.split(url)
            if len(items):
                url = items[0]
            else:
                # Forget about it
                return ''
            
        if (url.startswith('http:') or url.startswith('https:')):            
            try:
                return urlnorm.norms(url)
            except:
                return url

        # What about FTP ?
        if url.startswith('ftp:'):
            return urlnorm.norms(url)
        
        protocol,domain,path,dummy,dummy,dummy = urlparse.urlparse(sourceurl)
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

        try:
            url2 = urlnorm.norms(url)
        except:
            return url

        return url2
    
    def _fetch(self, url):
        """ Fetch a URL and return its data """

        self.driver.get(url)
        return self.driver.page_source

    def _crawl(self, url):
        """ Crawl URLs recursively upto configured level """

        if self.nurls >= self.maxurls:
            with self.mark('Max URLs limit reached. Parsing no more URLs.'):
                return

        urlhash = hashlib.md5(url).hexdigest()
        
        if urlhash in self.allurls:
            # print 'URL already crawled =>',url
            return

        print 'Fetching URL %s...' % url
        data = self._fetch(url)
        if not data:
            print 'No data,nothing to parse - %s' % url
            return

        self.nurls += 1
        
        parser = URLLister()
        print "Parsing URL %s" % url
        parser.feed(data)
        parser.close()

        self.allurls[urlhash] = 1
        self._content[url] = data

        child_urls = [self.makeProperURL(url, childurl) for childurl in parser.urls]
        # Remove anything from all URLs
        child_urls = [url for url in child_urls if url not in self.allurls and url.strip()]
        
        if self.strict:
            # Only pick up those URLs starting with root
            child_urls = filter(lambda x: x.lower().startswith(self.url_nop.lower()),
                                child_urls)

        for childurl in child_urls:
            # childurl is the actual URL
            self._crawl(childurl)

    @contextmanager
    def timer(self):
        """ Time stuff """

        t1 = time.time()
        yield
        t2 = time.time()
        print 'Time taken',t2 -t1,'seconds.'

    @contextmanager
    def mark(self, msg):
        """ Do something with flag set """

        if not self.flag:
            print msg
            yield
            self.flag = True
        else:
            yield
            
    def crawl(self):
        """ Crawl according to specifications """

        with self.timer():
            self._crawl(self.url)
            print 'Found',len(self.allurls),'URLs.'
            
    def get_content(self):
        """ Return all contents """

        return self._content

    def __del__(self):
        self.driver.quit()
    
if __name__ == "__main__":
    import sys
    selcrawler = SeleniumCrawler(sys.argv[1])
    selcrawler.crawl()
    
