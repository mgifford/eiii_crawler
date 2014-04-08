# -- coding: utf-8

"""
EIII prototype crawler.

Copyright (C) 2014 - Tingtun.
"""

import socket
gsocket = socket.socket

import Queue
import threading
import urllib
import urllib2
import requests
import httplib
import sgmllib
import urlparse
import time
import sys
import urlnorm
import random
import cStringIO
import md5 
import utils
import datetime
import traceback
import collections
import hashlib

from bs4 import BeautifulSoup
import re,htmlentitydefs
from threading import Semaphore
import robotstxt
# import FeatureExtraction
# import indexer
import lxml
import signal

from contextlib import contextmanager
import TingtunUtils.logger as logger
from TingtunUtils.fetchurl import fetchurl, geturl, FetchUrlException

___author__ = "Anand B Pillai"
__maintainer__ = "Anand B Pillai"
__version__ = "0.01 alpha"
__lastmodified__ = "Thu Mar 27 00:03:15 IST 2014"


msword_re = re.compile(r'microsoft\s+(word|powerpoint|excel)\s*-', re.IGNORECASE)
paren_re = r=re.compile('^\(|\)$')
# Regular expression for anchor tags
anchor_re = re.compile(r'\#+')

# socket.socket = gsocket
socket.setdefaulttimeout(120)
g_urltextmap = {}

class SmartwordTextProcessor(object):
    """ Class for identifying common prefix of pages in a site
    by sampling. Returns common prefix which can be removed in
    the word text for the site. Helpful in identifying and removing
    menu items from websites while crawling """
    
    __metaclass__ = utils.SingletonMeta

    def __init__(self):
        self.texts = []
        self.count = 0
        self.space_end = re.compile(r'\s+$')
        self.common_prefixes = {}
        
    def addText(self, text):
        """ Add texts for processing """
        
        self.texts.append(text)
        # Process every 5th time
        self.count += 1
        if self.count % 5 == 0:
            cprefix = self.process()
            if cprefix:
                self.common_prefixes[self.count] = cprefix
                
            # Reset
            self.texts = []

    def _commonPrefix(self, a, b):
        """ Return common prefix of two strings a & b """

        try:
            common = a[:[x[0]==x[1] for x in zip(a,b)].index(0)]
        except ValueError:
            # Means both strings are same
            return None
        
        # Return till last space character
        try:
            if self.space_end.search(common):
                return common
            else:
                return common[:common.rindex(' ')].strip()
        except ValueError:
            return common

    def process(self):
        """ Process text looking for common content """

        # Shuffle and pick top half
        prefixes = []
        
        for x in range(len(self.texts)):
            random.shuffle(self.texts)
            # Pick two texts
            text1, text2 = self.texts[0], self.texts[1]
            prefix = self._commonPrefix(text1, text2)
            if prefix:
                prefixes.append(prefix)

        if len(prefixes)==0:
            return ''
        
        unique = set(prefixes)
        counts = {}

        for prefix in unique:
            counts[prefix] = 100.0*prefixes.count(prefix)/len(prefixes)
            
        return sorted(counts, key=counts.get, reverse=True)[0]

    def getCommonPrefix(self):
        """ Return most commonly occuring common prefix """
        
        # Do this if at least two tests have been performed
        if self.count >= 5:
            prefixes = self.common_prefixes.values()
            unique = set(prefixes)

            counts = {}

            for prefix in unique:
                counts[prefix] = 100.0*prefixes.count(prefix)/len(prefixes)
            
            # return sorted(counts, key=counts.get, reverse=True)[0]
            allprefixes = list(set(counts.keys()))
            # Sort by decreasing length
            return sorted(allprefixes, key=len, reverse=True)
        else:
            return None
    
    
class UrlTextDict(object):
    __metaclass__ = utils.SingletonMeta

    def __init__(self):
        self.urldict = {}
        # full URL to relative URL mapping
        self.urlmap = {}

    def update(self, urlmap):
        self.urldict.update(urlmap)

    def getText(self, url):

        # If url has '/' at end try without it
        # else vice-verza
        if url[-1] == '/':
            url2 = url[:-1]
        else:
            url2 = url + '/'

        for item in (url, url2):
            text = self.urldict.get(item, None)
            if text:
                return text
        
        # The URL may not have been full - so
        # try with original URL
        for item in (url, url2):
            origurl = self.urlmap.get(item, None)
            if origurl:
                text = self.urldict.get(origurl, None)
                if text: return text

    def setText(self, url, text):
        self.urldict[url] = text

    def setUrlRelation(self, origurl, fullurl):
        # Full URL is the key
        self.urlmap[fullurl] = origurl

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
            
class BeautifulLister(object):
    """ An HTML parser using BeautifulSoup """
    
    def __init__(self, content):
        soup=BeautifulSoup(content, "lxml")
        # Text of links mapped to the link text
        self.urlmap = {item['href']:item.text for item in soup.findAll('a') if item.has_key('href')}
        self.urls = self.urlmap.keys()
        self.urldict = {}

    def feed(self, content):
        # Dummy method
        pass

    def close(self):
        # Dummy method
        pass
    
    def reset(self):
        self.urls = []

    def getUrlMap(self):
        return self.urlmap

class CrawlerBase(object):
    """ Base class for Crawler classes """

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

class CrawlerThread(threading.Thread, CrawlerBase):
    """ Crawler worker class which performs the task of downloading,
    parsing and indexing content """

    def __init__(self,
                 parent,
                 downloadqueue,
                 downloaded,
                 couldnotdownload,
                 dburls,
                 site,
                 dbMutex,
                 useragent,
                 delay,
                 deadlinks,
                 foundsublinks,
                 contenttype_filter='',
                 recrawl=False,
                 restrictlevel=None,
                 suburls=False,
                 rooturl=None,
                 strict=False,
                 downloadctype=[]):
        """ Initializer - Accepts downladqueue, download-failed list, SearchDB instance,
        database urls list, site, database write mutex, user-agent, delay in seconds,
        list of dead-links, list of sub-links, recrawl flag, night-only flag,
        faqs-only flag, faq-option, restrict level option, content-type list """

        threading.Thread.__init__(self)
        CrawlerBase.__init__(self)
        
        self.parent = parent
        self.daemon = True
        self.downloadqueue = downloadqueue
        self.downloaded = downloaded
        self.couldnotdownload = couldnotdownload
        self.dburls = dburls
        self.deadlinks = deadlinks
        self.site = site
        self.suburls = suburls
        # No of levels of crawling restriction
        self.restrictlevel = restrictlevel
        self.stopparse = False

        # Content type filter
        self.contenttype_filter = []
        if contenttype_filter:
            self.contenttype_filter = [self.guessContentType('http://www.foo.com/test.' + extn) for \
                                       extn in contenttype_filter.split(',') if extn]

        print 'Content-type filter=>',self.contenttype_filter
        self.cycle = 0
        self.conn = None
        self.stopnow = False
        self.strict = strict
        self.iswaiting = False
        self.dbMutex = dbMutex
        self.useragent = useragent
        self.delay = float(delay)
        self.htmlctypes = ['text/html', 'application/xhtml+xml', 'application/xml', 'text/xml']
        self.pdfctypes = ['application/pdf',]
        # doc, docx, ppt, pptx, xls, xlsx
        self.msctypes = ['application/msword',
                         'application/vnd.ms-powerpoint',
                         'application/vnd.ms-excel',                                                 
                         'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                         'application/vnd.openxmlformats-officedocument.presentationml.presentation',
                         'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                         ]
        
        # odt, odp and ods
        self.odftypes = ['application/vnd.oasis.opendocument.text',
                         'application/vnd.oasis.opendocument.presentation',
                         'application/vnd.oasis.opendocument.spreadsheet']      
                         
        self.documentctypes = self.pdfctypes + self.msctypes + self.odftypes
        # Smart word processor
        self.smartwordproc = SmartwordTextProcessor()
        # Common prefix
        self.commonprefix = None
        # Restrict downloading to a specific content-type
        self.downloadctype = downloadctype
        # Optional root url
        self.rooturl = rooturl
        if len(self.downloadctype)==0:
            self.downloadctype = self.htmlctypes + self.documentctypes
        self.recrawl = recrawl
        self.sigcount = 0
        self.foundsublinks = foundsublinks
        # Install signal handlers
        signal.signal(signal.SIGINT, self.sighandler)
        signal.signal(signal.SIGTERM, self.sighandler)        

    def sighandler(self, signum, stack):
        """ Signal handler """

        if signum in (signal.SIGINT, signal.SIGTERM,):
            utils.info('Got signal, stopping...')
            self.stopnow = True
            self.sigcount += 1

        if self.sigcount>1:
            utils.info('Exiting.')
            # Not clean exit
            sys.exit(1)
            
    @classmethod
    def getUrlDir(self, listofurls):
        """ Return the URL path that is one path up of the current URL.
        E.g: http://www.foo.com/bar/vodka/ => http://www.foo.com/bar/ """

        # Only do this if URL has no query or params
        newurls = []
        
        for url in listofurls:
            # If this is not HTML, skip it
            urlp = urlparse.urlparse(url)
            if urlp.params or urlp.query or urlp.fragment:
                continue
            else:
                path = urlp.path
                if path:
                    # If this is not HTML, skip it
                    splitpath = path.rsplit('.', 1)
                    if len(splitpath)==2: continue
                    
                    paths = [item for item in path.split('/') if item]
                    if len(paths)>=2:
                        paths = '/' + '/'.join(paths[:-1])
                        newurl = urlparse.urlunparse(urlparse.ParseResult(scheme=urlp.scheme,
                                                                          netloc=urlp.netloc,
                                                                          path=paths,
                                                                          fragment='',
                                                                          query='',
                                                                          params=''))
                        newurls.append(newurl)
                else:
                    # Nothing to do
                    pass

        return newurls
                
    def guessContentType(self, url):
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
        
    def run(self):
        """ Overridden 'run' method """
        
        try:
            self.doWork()
        finally:
            # Make sure the mutex is released in case of any unhandled exception
            self.dbMutex.release()

    def doWork(self):
        """ Main routine of the class - performs downloading, parsing
        of content, fetching new links and indexing downloaded content """
        
        while True:
            try:
                reallydownloaded = [i[1] for i in self.downloaded]
                # reallydownloaded = []
            except:
                reallydownloaded = []
            self.iswaiting = True

            # print reallydownloaded
            ctype,urlkey,linkedfromurl,crawlbelow = self.downloadqueue.get()
            # original URL
            url = urlkey
            origurl = url

            flag = True
            
            while flag:
                nodownload = (url in self.couldnotdownload) and (not (url in reallydownloaded))
                yesdownload = (url in reallydownloaded)
                if nodownload:
                    utils.debug('Ignoring URL,',url,', because could not download')
                elif yesdownload:
                    utils.debug('Ignoring URL,',url,', because already downloaded')
                else:
                    break

                flag = (nodownload or yesdownload)
                ctype,url,linkedfromurl, crawlbelow = self.downloadqueue.get()

            self.iswaiting = False
            # print 'No longer sleeping'
            time.sleep(1)

            if (not url) or self.stopnow:
                utils.info('Time to exit')
                break

            # Skip anything other than HTML for kontakt info crawling
            if ctype in self.htmlctypes + self.documentctypes:
                try:
                    utils.info('Downloading ',url,'...')
                except:
                    pass
                
                title = ''

                try:
                    url = url.replace(' ','%20')

                    # We are using TingtunUtils:fetchurl::geturl API
                    f = geturl(url)
                    prevurl = url
                    url = f.url

                    if not url==prevurl:
                        self.couldnotdownload.add(prevurl)

                    # Bug - requests library doesn't raise errors for 404
                    # So we need to look at status code and raise the
                    # exception ourselves - generic catch all for all codes other
                    # than 200
                    if f.status_code in range(400, 420) or f.status_code>=500:
                        # Raise exception ourselves so that this
                        # gets added to dead links with right message
                        raise requests.exceptions.RequestException,"%d: %s" % (f.status_code,
                                                                              f.reason)
                    
                    try:
                        ctype = f.headers['content-type'].split(';')[0]
                    except KeyError:
                        # Assume HTML
                        ctype = 'text/html'
                        
                    content = f.content

                    utils.info('Downloaded ',url,'with content-type',ctype,'.')                 
                    
                    if ctype in self.htmlctypes and (len(content)/(1024*1024))<2:
                        # Removed all SE data extraction stuff - add code here later
                        pass

                    elif ctype in self.documentctypes:
                        # Removed all SE data extraction stuff - add code here later.
                        pass

                # Catch a bunch of network errors - courtesy havestman
                except (requests.exceptions.RequestException,
                        urllib2.HTTPError,urllib2.URLError,
                        httplib.BadStatusLine,IOError,TypeError,
                        ValueError, AssertionError,
                        socket.error, socket.timeout), e:
                    # raise
                    content = ''
                    title = ''
                    detecteddate = ''
                    newcontent = ''
                    contactinfo = {}
                    metawords = {}
                    faqContent = ''
                    keywords = []
                    utils.error('Error downloading URL:',url,e,linkedfromurl)
                    self.couldnotdownload.add(url)                    
                    self.deadlinks.put((url,linkedfromurl,str(e)))
                except Exception, e:
                    # Catch anything else below, but dont count
                    # as a network error
                    # raise
                    content = ''
                    title = ''
                    detecteddate = ''
                    newcontent = ''
                    contactinfo = {}
                    metawords = {}
                    faqContent = ''
                    keywords = []
                    utils.error('Exception downloading URL:',url,e,linkedfromurl)
                    self.couldnotdownload.add(url)

                if self.delay:
                    realdelay = random.uniform(self.delay*0.5, self.delay*1.5)
                    time.sleep(realdelay)
                    
                self.dbMutex.acquire()
                self.downloaded.add((ctype,url,content))

                if title:
                    utils.info('Title =>',title,type(title))
                    # Remove any parens from beginning and end
                    # Remove microsoft prefixes
                    title = paren_re.sub('', msword_re.sub('', title)).strip()
                else:
                    utils.debug('NO TITLE!')
                
                if (type(title)==type('') or type(title)==type(u'')) and len(title)>1024:
                    title = title[0:1024]

                crawl_flag, suburl_flag = True, False
                # Check crawlbelow from kontakt crawl settings
                crawlbelow = self.parent.starturldict.get(origurl, [True,False])[0]
                suburl_flag = self.parent.starturldict.get(origurl, [True,False])[1]                
                    
                # If strict, crawl flag is False
                if self.strict or (not crawlbelow):
                    utils.info('Strict setting, not crawling at all.')
                    crawl_flag = False

                    # EDITNOTE: Not putting to dburls since write action not defined yet.
                    
                    # self.dburls.put((ctype,utils.cleanurl(url),content))
                    
                elif ctype in self.htmlctypes + self.documentctypes:
                    # print 'CRAWL FLAG',crawl_flag,'=>',url

                    # EDITNOTE: Not putting to dburls since write action not defined yet.
                    
                    # self.dburls.put((ctype,utils.cleanurl(url),content))
                    pass
                     
                self.dbMutex.release()
                newurls = []

                restrictlevel = 0
                if self.restrictlevel:
                    restrictlevel = self.restrictlevel
                    
                # Restrict level is set - then check and exit
                if restrictlevel:
                    if self.cycle >= restrictlevel:
                        utils.info('Number of cycles limit reached ( %d ), not parsing any more links' % restrictlevel)
                        self.stopparse = True
                
                if ctype in self.htmlctypes and (not self.stopparse) and crawlbelow and crawl_flag:
                    parser = URLLister()

                    try:
                        utils.info("Parsing URL",url)
                        parser.feed(content)
                        parser.close()
                    except sgmllib.SGMLParseError:
                        utils.error('Error parsing:',url)

                    try:
                        newurldict = {childurl:self.makeProperURL(url,childurl) for childurl in parser.urls}
                        # Set the URL dict keys again for the new URLs
                        for childurl in newurldict:
                            newurl = newurldict[childurl]
                            
                            if childurl in parser.urldict:
                                parser.urldict[newurl] = parser.urldict[childurl]
                                del parser.urldict[childurl]

                        # print 'URLMAP:',parser.urldict
                        newurls = newurldict.values()
                        # Mapping of URLs to the text.
                        g_urltextmap.update(parser.urldict)
                        
                        # If suburls is defined and URL doesn't match
                        # parent, return ''
                        newurls2 = []
                        compurl = None
                        
                        if self.suburls or suburl_flag:
                            if self.rooturl:
                                rooturl = self.rooturl
                                
                            if rooturl:
                                compurl = rooturl
                            else:
                                compurl = url

                        # print 'Comparison URL is',compurl
                        for newurl in newurls:
                            if compurl and (not newurl.lower().startswith(compurl.lower())):
                                utils.debug(newurl,'does not match',compurl,'skipping...')
                                continue
                            newurls2.append(newurl)

                        newurls = newurls2
                        
                        # Add directory of URLs
                        if 1:
                            dirurls = self.getUrlDir(newurls)
                            if self.suburls or suburl_flag:
                                if self.rooturl:
                                    compurl = self.rooturl
                                else:
                                    compurl = url

                                for dirurl in dirurls:
                                    if dirurl.lower().startswith(compurl.lower()):
                                        newurls.append(dirurl)
                                    else:
                                        utils.debug('Dir URL',dirurl,'does not match',compurl,'skipping...')
                            else:
                                newurls += dirurls

                    except Exception:
                        raise
                        utils.error('Malformed URLs:',parser.urls)
                        newurls = []

                else:
                    utils.debug('Not crawling',url)

                if len(newurls):
                    self.dbMutex.acquire()
                    tempdownloaded = self.downloaded.copy()
                    self.dbMutex.release()
                    tempdownloaded = [i[1] for i in tempdownloaded]

                    # Remove duplicates
                    newurls = list(set(newurls))
                    # For kontakt info crawl, skip inclusionexclusion check for external domains.

                    # EDITNOTE: Removed inclusion exclusion check here (searchdb.performInclusionExclusion(...))
                    # Might need something like that later.
                    newurls = [newurl for newurl in newurls if newurl and newurl not in tempdownloaded]

                    if len(newurls):
                        utils.info('Discovered',len(newurls),'urls within scope')

                    # print 'Downloadctype=>',self.downloadctype
                    newurls = [(self.guessContentType(newurl), newurl, url, True) for newurl in newurls]
                    # Filter through content-type filter
                    if self.contenttype_filter:
                        newurls = [x for x in newurls if x[0] not in self.contenttype_filter]
                        
                    for ctype, newurl, url, stuff in newurls:
                        if ctype not in self.downloadctype:
                            utils.info('Omitting URL',newurl,'=>',ctype)

                    if not self.recrawl:
                        self.cycle += 1
                        if len(newurls):
                            utils.debug('Putting',len(newurls),self.cycle,self.restrictlevel,newurls[0],self.downloadctype)
                        # print 'NEWURLS=>',newurls
                        x = [self.downloadqueue.put(newurl) for newurl in newurls if newurl[0] in self.downloadctype]
                        if content:
                            self.foundsublinks.put(utils.cleanurl(url))
  
    def stop(self):
        """ Stop the crawler thread """
        
        self.stopnow = True
        self.downloadqueue.put((None,None,None,False))

class CustomLevelCrawler(CrawlerBase):
    """ Crawler that crawls upto 'n' levels from the
    starting URL. 'n' is a parameter to the crawler class """

    def __init__(self, url, n=1, strict=False, use_params=True, maxurls=50):
        """ Crawl from starting URL 'url' upto 'n' levels """
        self.url = url
        self.maxlevel = n
        self.maxurls = maxurls
        
        self.level = 0
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
        
    def _fetch(self, url):
        """ Fetch a URL and return its data """

        try:
            content, headers = fetchurl(url)
            return content
        except FetchUrlException, e:
            print e

    def _crawl(self, url):
        """ Crawl URLs recursively upto configured level """

        if self.nurls >= self.maxurls:
            with self.mark('Max URLs limit reached. Parsing no more URLs.'):
                return

        print '(%d) - Fetching URL %s...' % (self.level, url)
        data = self._fetch(url)
        if not data:
            print '(%d) No data,nothing to parse - %s' % (self.level, url)
            return

        self.nurls += 1
        
        parser = URLLister()
        print "(%d) Parsing URL %s" % (self.level, url)
        parser.feed(data)
        parser.close()

        if url not in self.allurls:
            self.url_levels[self.level] += 1

        self.allurls[url] = 1
        self._content[url] = data

        if self.level == self.maxlevel:
            msg = 'Crawl level %d reached. Parsing no more URLs.' % self.maxlevel
            with self.mark(msg):
                return

        child_urls = [self.makeProperURL(url, childurl) for childurl in parser.urls]
        # Remove anything from all URLs
        child_urls = [url for url in child_urls if url not in self.allurls if url.strip()]
        
        if self.strict:
            # Only pick up those URLs starting with root
            child_urls = filter(lambda x: x.lower().startswith(self.url_nop.lower()),
                                child_urls)

        for childurl in child_urls:
            self.level += 1
            # childurl is the actual URL
            self._crawl(childurl)
            self.level -= 1

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
            print 'Found',len(self.allurls),'URLs at',self.maxlevel,'levels.'
            
            for level,nurls in self.url_levels.items():
                print '\t %d URLs at level %d' % (nurls, level)

                
    def get_content(self):
        """ Return all contents """

        return self._content
    
class Crawler(object):
    """ The Crawler class """
    
    def __init__(self,
                 site,
                 urls,
                 contenttype,
                 contenttype_filter = '',
                 recrawlurls = [],
                 ignoretimelimit=False,
                 urlpattern=None,
                 verbose=False,
                 suburls=False,
                 restrictlevel=None,
                 rooturl=None,
                 strict=False,
                 ignorerules=False,
                 testrunid=0):

        """ Initializer - accepts site, testrun id, content-type, 
        recrawl urls if any, night-only flag, faqs-only flag, ignore-timelimit flag,
        manual-urls only flag, verbose flag, restrictlevel option """

        self.site=site
        self.dbMutex = Semaphore(1)
        self.downloaded = set()
        self.couldnotdownload = set()
        self.downloadqueue = Queue.Queue()
        self.deadlinks = Queue.Queue()
        self.urlpattern = urlpattern
        # Start URL dict
        self.starturldict = {}      
        # Download only URLs below the path ?
        # E.g http://www.foo.com/bar => http://www.foo.com/bar/beer
        self.suburls = suburls
        self.rooturl = rooturl
        self.verbose = verbose
        self.restrictlevel = restrictlevel
        self.dburls = Queue.Queue()#set()
        self.contenttype = contenttype
        # Delay is hard-coded to 1.0 sec
        self.delay = 1.0
        # Will possibly need this for multiple sites
        self.robotstxt = robotstxt.robotstxt(self.site)
        self.foundsublinks = Queue.Queue()
        self.htmlpages = 10000
        self.pdfpages = 200
        # User-agent
        self.useragent = 'Mozilla/5.0'
        if self.contenttype=='HTML':
            self.pagestodownload = self.htmlpages
        elif self.contenttype=='PDF':
            self.pagestodownload = self.pdfpages
        else:
            # Fixed
            self.pagestodownload = 500

        self.testrunid = testrunid
        self.strict = strict
        # Number of URLs
        self.numtotalurls = 0
        # Number of URLs failed
        self.numtotalfailed = 0
        # Ignore Rules when writing
        self.ignorerules = ignorerules
        
        downloadctype = []

        self.numthreads = 5

        url_dict = {}
        
        self.crawlerthreads = [CrawlerThread(self,
                                             self.downloadqueue,
                                             self.downloaded,
                                             self.couldnotdownload,
                                             self.dburls,
                                             self.site,
                                             self.dbMutex,
                                             self.useragent,
                                             self.numthreads*self.delay,
                                             self.deadlinks,
                                             self.foundsublinks,
                                             contenttype_filter,
                                             recrawlurls,
                                             restrictlevel=self.restrictlevel,
                                             suburls=self.suburls,
                                             downloadctype=downloadctype,
                                             rooturl=self.rooturl,
                                             strict=self.strict) for i in range(self.numthreads)]
        [i.start() for i in self.crawlerthreads]

        if recrawlurls:
            for contenttype,url in recrawlurls:
                self.downloadqueue.put((contenttype,url,'',True))

            while not (self.queueempty()):
                # print 'Waiting:',time.time(),len(self.downloaded),self.downloadqueue.qsize(),self.dburls.qsize()
                if self.dburls.qsize()>random.randint(1,25):
                    self.writeToDB()
                time.sleep(5)
        else:
            self.starturldict = {url:(True, False) for url in urls}

            # print 'START URL DICT =>',self.starturldict
            utils.info('Start urls are =>', urls)
                
            for url in urls:
                if url:
                    try:
                        utils.info('Starting to crawl from url',url,'for site',site)
                    except:
                        pass
                    self.downloadqueue.put(('text/html',url,'', url_dict.get(url,True)))

            emptycount = 0
            time.sleep(10)
            
            while not (self.queueempty() or self.enoughalready(self.contenttype)):
                emptycount += 1
                # print 'Waiting:',time.time(),len(self.downloaded),self.downloadqueue.qsize(),self.dburls.qsize()
                if emptycount % 50 == 0:
                    print 'Waiting:',time.time(),len(self.downloaded),self.downloadqueue.qsize(),self.dburls.qsize()
                    
                # sys.stdout.flush()
                if self.dburls.qsize()>random.randint(1,50):
                    self.writeToDB()

                time.sleep(5)
                if self.queueempty():
                    if emptycount > 10:
                        utils.info('Exiting loop')
                        break
                  
        sys.stdout.flush()
        utils.debug('Should be finished ',self.queueempty(),self.enoughalready(self.contenttype),(self.queueempty() or self.enoughalready(self.contenttype)),not (self.queueempty() or self.enoughalready(self.contenttype)))
        sys.stdout.flush()
        utils.info("Stopping")
        sys.stdout.flush()
        [i.stop() for i in self.crawlerthreads]
        utils.info("Writing to DB")
        sys.stdout.flush()
        self.writeToDB()
        # Write crawl stats
        utils.info("Updating crawl stats.")
        sys.stdout.flush()
        utils.info('Crawl done.')
        sys.exit(0)

    def writeToDB(self):
        """ Write the indexed content to database """

        # EDITNOTE: Removed all writes to DB.
        pass

    def queueempty(self):
        """ Check method - Check if the download queue is empty """
        
        utils.debug('Checking empty queue')
        sys.stdout.flush()
        for crawlerthread in self.crawlerthreads:
            if crawlerthread.iswaiting == False:
                return False
            
        utils.debug('Finished checking empty queue')
        sys.stdout.flush()
        return self.downloadqueue.empty() # and self.dburls.empty()

    def enoughalready(self,contenttype):
        """ Check method - Check if we have downloaded enough content already
        according to the limits prescribed in the configuration """
        
        utils.debug('Checking enough URLs already')
        sys.stdout.flush()
        self.dbMutex.acquire()

        htmlnumdownloaded = len([i for i in self.downloaded if 'html' in i[0].lower()])
        pdfnumdownloaded = len([i for i in self.downloaded if 'pdf' in i[0].lower()])

        utils.debug('htmldownloaded ' + str(htmlnumdownloaded))
        self.dbMutex.release()
        utils.debug('Finished checking enough URLs already')

        sys.stdout.flush()

        if contenttype=='HTML':
            return htmlnumdownloaded > int(self.htmlpages)
        if contenttype=='PDF':
            return (pdfnumdownloaded > int(self.pdfpages)) or ((htmlnumdownloaded+pdfnumdownloaded)>(int(self.htmlpages)+int(self.pdfpages)))

    

if __name__ == "__main__":
    cr = Crawler('http://www.tingtun.no',
                 ['http://www.tingtun.no'],
                 'HTML')
    
