""" Implementation of EIII crawler """

import crawlerbase
import sys
import Queue
import requests
import urlhelper
import robocop
import urllib
import urlparse
import signal
import bitmap
import binascii

class EIIICrawlerUrlData(crawlerbase.CrawlerUrlData):
    """ Class representing downloaded data for a URL.
    The data is downloaded using the TingtunUtils fetcher """

    def download(self, crawler):
        """ Overloaded download method """
        
        try:
            self.content, self.headers = urlhelper.fetch_url(self.url, {'user-agent': self.useragent})
            # Notify the crawler of download
            # Later - implement events for this
            crawler.notify_download(self.url, self.content, self.headers)
        except urlhelper.FetchUrlException, e:
            print 'Error downloading',self.url,'=>',str(e)
            crawler.notify_download_error(self.url, str(e))
            
    def get_data(self):
        """ Return the data """
        return self.content

    def get_headers(self):
        """ Return the headers """
        return self.headers
    
class EIIICrawlerQueuedWorker(crawlerbase.CrawlerWorkerBase):
    """ EIII Crawler worker using a shared FIFO queue as
    the data structure that is used to share data """
    
    def __init__(self, config, manager):
        self.manager = manager
        self.stop_now = False
        # Signal count
        self.sig_count = 0
        # Robots parser
        self.robots_p = robocop.Robocop(useragent=config.get_real_useragent())
        # Install signal handlers
        signal.signal(signal.SIGINT, self.sighandler)
        signal.signal(signal.SIGTERM, self.sighandler)              
        
        super(self.__class__,  self).__init__(config)

    # Notification methods for event handling from sub-objects
    # START: Notification methods
    def notify_download(self, url, content, headers):
        """ Notification that download is complete. """

        self.manager.url_download_complete(url, content, headers)

    def notify_download_error(self, url, error):
        """ Notification that download did not complete """

        self.manager.url_download_error(url, error)

    # END: Notification methods
    
    def sighandler(self, signum, stack):
        """ Signal handler """

        if signum in (signal.SIGINT, signal.SIGTERM,):
            print 'Got signal, stopping...'
            self.stop_now = True
            self.sig_count += 1

        if self.sig_count>2:
            print 'Force Quitting...'
            # Not exited in natural course, force exiting.
            sys.exit(1)

    def get(self, timeout=10):
        """ Get the data to crawl """

        return self.manager.get(timeout=timeout)

    def push(self, data):
        """ Push new data to crawl """

        return self.manager.put(data)
    
    def parse_queue_urls(self, data):
        """ Parse the URL data from the queue and return a 3-tuple
        of content-type, URL, parent-URL """

        # In this case we return directly as the data that is
        # pushed exactly match this structure
        return data

    def get_url_data_instance(self, url):
        """ Make an instance of the URL data class
        which fetches the URL """

        return EIIICrawlerUrlData(url, self.config)

    def build_url(self, url, parent_url):
        """ Build the complete URL for child URL using the parent URL """

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

        try:
            url2 = urlnorm.norms(url)
        except:
            return url

        return url2        
                  
    def parse(self, data, url):
        """ Parse the URL data and return an iterator over child URLs """

        parser = urlhelper.URLLister()

        try:
            print "Parsing URL",url
            parser.feed(data)
            parser.close()
        except sgmllib.SGMLParseError, e:
            print "Error parsing data for URL =>",url

        urls = list(set(parser.urls))
        return urls

    def should_stop(self):
        """ Should stop now ? """

        return self.stop_now
    
    def work_pending(self):
        """ Whether crawl work is still pending """

        # Is the queue empty ?
        print 'Checking work pending...',
        result = self.manager.is_empty()
        if result:
            print 'No.'
        else:
            print 'Yes.'

        return (not result)
            
    def allowed(self, url, parent_url=None, content=None, content_type='text/html', headers={}):
        """ Is fetching of URL allowed ? """

        # Is already downloaded ? Then skip right away
        # NOTE - Do this only for child URLs!
        if (parent_url != None) and self.manager.check_already_downloaded(url):
            print url,'=> already downloaded'
            return False
        
        # print 'Checking allowed : ',url,'<=>',parent_url
        if (content != None) or len(headers):
            # Do content or header checks
            # print 'Checking content rules...'
            return self.check_content_rules(url, parent_url, content, content_type, headers)
        
        # Blanket True as of now
        # Check robots.txt
        if not self.flag_ignorerobots:
            self.robots_p.parse_site(url)
            # NOTE: Don't check meta NOW since content of URL has not been downloaded yet.
            if not self.robots_p.can_fetch(url, content=content, meta=False):
                # print 'Robots.txt rules disallows URL =>',url
                return False

        # Scoping rules
        if parent_url != None:
            scoper = crawlerbase.CrawlerScopingRules(self.config, parent_url)

            # Proceed further - do site scoping rules
            if not scoper.allowed(url):
                # print 'Scoping rules does not allow URL=>',url
                return False
            
        return True

    def check_content_rules(self, url, parent_url=None, content=None, content_type='text/html', headers={}):
        """ Fetching of URL allowed by inspecting the content and headers (optional) of the URL.
        Returns True if fine and False otherwise """

        # Yes - this is a bit ironic, but some rules work just like this.
        # for example the NOINDEX of the META robots tags. They tell you
        # whether content can be indexed or followed AFTER downloading the
        # content.

        if self.flag_metarobots:
            index, follow = self.robots_p.check_meta(url, content=content)
            # Don't bother too much with NO index, but bother with NOFOLLOW
            if not follow:
                return False

        # Not doing any other content rules now
        return True
        
class EIIICrawler(object):
    """ EIII Web Crawler """

    def __init__(self, urls):
        self.config = crawlerbase.CrawlerConfig()
        self.urls = urls
        self.empty_count = 0
        # Download queue
        self.dqueue = Queue.Queue()
        # Bitmap instance for keeping record of
        # downloaded URLs
        self.url_bitmap = {}

    def get(self, timeout=10):
        """ Return the data for crawling """

        try:
            return self.dqueue.get(False, timeout=timeout)
        except Queue.Empty:
            print 'Queue Empty!'
            self.empty_count += 1

    def put(self, data):
        """ Push further data to be crawled """

        print 'Putting data...',
        self.dqueue.put(data)
        print ' put data.'

    def is_empty(self):
        """ Is the work queue empty ? """

        return self.empty_count>2

    def check_already_downloaded(self, url):
        """ Is a URL already downloaded """

        return self.url_bitmap.has_key(url)
    
    def url_download_complete(self, url, content, headers):
        """ Event callback for notifying download for a URL is done """

        # Mark in bitmap
        print 'Making entry for URL',url,'in bitmap...'
        self.url_bitmap[url] = 1

    def url_download_error(self, url, errmsg):
        """ Event callback for notifying download for a URL in error """

        # Mark in bitmap
        print 'Making entry for URL',url,'in bitmap...'     
        self.url_bitmap[url] = 1
        
    def crawl(self):
        """ Do the actual crawling """
        
        # Push the URLs to queue
        for url in self.urls:
            self.dqueue.put(('text/html',url,None))
            
        worker = EIIICrawlerQueuedWorker(self.config, self)
        worker.start()
        worker.join()
        print 'Crawl done.'


if __name__ == "__main__":
    crawler = EIIICrawler(['http://www.tingtun.no'])
    crawler.crawl()
