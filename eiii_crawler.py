""" Implementation of EIII crawler """

import crawlerbase
from crawlerbase import CrawlerEventRegistry
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
import re
import time
import logger

msword_re = re.compile(r'microsoft\s+(word|powerpoint|excel)\s*-', re.IGNORECASE)
paren_re = r=re.compile('^\(|\)$')

log = logger.getMultiLogger('eiii_crawler','crawl.log','crawl.err',console=True)

class EIIICrawlerUrlData(crawlerbase.CrawlerUrlData):
    """ Class representing downloaded data for a URL.
    The data is downloaded using the TingtunUtils fetcher """

    def build_headers(self):
        """ Build headers for the request """

        # User-agent is always sent
        headers = {'user-agent': self.useragent}
        for hdr in self.config.client_standard_headers:
            val = getattr(self.config, 'client_' + hdr.lower().replace('-','_'))
            headers[hdr] = val

        return headers

    def download(self, crawler):
        """ Overloaded download method """

        eventr = CrawlerEventRegistry.getInstance()
        
        try:
            freq = urlhelper.get_url(self.url, headers = self.build_headers())

            self.content = freq.content
            self.headers = freq.headers

            # requests does not raise an exception for 404 URLs instead
            # it is wrapped into the status code
            if freq.status_code == 200:
                eventr.publish(self, 'download_complete',
                               message='URL has been downloaded successfully',
                               code=200,
                               params=self.__dict__)
            else:
                eventr.publish(self, 'download_error',
                               message='URL has not been downloaded successfully',
                               code=freq.status_code,
                               params=self.__dict__)             
        except urlhelper.FetchUrlException, e:
            log.error('Error downloading',self.url,'=>',str(e))
            # FIXME: Parse HTTP error string and find out the
            # proper code to put here if HTTPError.
            eventr.publish(self, 'download_error',
                           message=str(e),
                           is_error = True,
                           code=0,
                           params=self.__dict__)

            
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
        # Robots parser
        self.robots_p = robocop.Robocop(useragent=config.get_real_useragent())
        # Event registry
        self.eventr = CrawlerEventRegistry.getInstance()
        super(self.__class__,  self).__init__(config)

    # END: Notification methods
    
    def get(self, timeout=30):
        """ Get the data to crawl """

        data = self.manager.get(timeout=timeout)
        # Each data is a URL, so raise the event
        # 'obtained_url' here.
        self.eventr.publish(self, 'url_obtained',
                            params=locals())
        return data

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

        builder = urlhelper.URLBuilder(url, parent_url)
        return builder.build()
    
    def parse(self, data, url):
        """ Parse the URL data and return an iterator over child URLs """

        parser = urlhelper.URLLister()

        try:
            log.debug("Parsing URL",url)
            parser.feed(data)
            parser.close()

            self.eventr.publish(self, 'url_parsed',
                                params=locals())
            
        except sgmllib.SGMLParseError, e:
            log.error("Error parsing data for URL =>",url)

        urls = list(set(parser.urls))
        return urls

    def should_stop(self):
        """ Should stop now ? """

        return self.stop_now

    def stop(self):
        """ Forcefully stop the crawl """

        self.stop_now = True
        
    def work_pending(self):
        """ Whether crawl work is still pending """

        # Is the queue empty ?
        log.debug('Checking work pending...',)
        result = self.manager.is_empty()
        if result:
            log.debug('No.')
        else:
            log.debug('Yes.')

        return (not result)

    def allowed(self, url, parent_url=None, content=None, content_type='text/html', headers={}):
        """ Is fetching of URL allowed ? """        

        # NOTE: This is a wrapper over the actual function _allowed which does all
        # the work. This is to allow publication of events after capturing the return
        # value of the method.
        result = self._allowed(url, parent_url=parent_url, content=content,
                               content_type=content_type, headers=headers)

        if not result:
            # Filtered
            self.eventr.publish(self, 'url_filtered',
                                params=locals())

        return result
        
    def _allowed(self, url, parent_url=None, content=None, content_type='text/html', headers={}):
        """ Is fetching of URL allowed ? - Actual Implementation """                

        # Skip mime-types we don't want to deal with based on URL extensions
        guess_ctype = urlhelper.guess_content_type(url)
        if guess_ctype not in self.config.client_mimetypes:
            log.debug('Skipping URL',url,'as content-type',guess_ctype,'is not valid.')
            return False
        
        # Is already downloaded ? Then skip right away
        # NOTE - Do this only for child URLs!
        if (parent_url != None) and self.manager.check_already_downloaded(url):
            log.debug(url,'=> already downloaded')
            return False
        
        if (content != None) or len(headers):
            # Do content or header checks
            return self.check_content_rules(url, parent_url, content, content_type, headers)
        
        # Blanket True as of now
        # Check robots.txt
        if not self.flag_ignorerobots:
            self.robots_p.parse_site(url)
            # NOTE: Don't check meta NOW since content of URL has not been downloaded yet.
            if not self.robots_p.can_fetch(url, content=content, meta=False):
                log.debug('Robots.txt rules disallows URL =>',url)
                return False

        # Scoping rules
        if parent_url != None:
            scoper = crawlerbase.CrawlerScopingRules(self.config, parent_url)

            # Proceed further - do site scoping rules
            if not scoper.allowed(url):
                log.debug('Scoping rules does not allow URL=>',url)
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

    def __init__(self, urls, cfgfile='config.json'):
        # Load config from file.
        self.config = crawlerbase.CrawlerConfig.fromfile(cfgfile)
        self.urls = urls
        self.empty_count = 0
        # Download queue
        self.dqueue = Queue.Queue()
        # Bitmap instance for keeping record of
        # downloaded URLs
        self.url_bitmap = {}
        # Crawl stats
        self.stats = crawlerbase.CrawlerStats()
        # Crawl limits enforcement
        self.limit_checker = crawlerbase.CrawlerLimitRules(self.config)
        # Event registry
        self.eventr = CrawlerEventRegistry.getInstance()
        self.subscribe_events()
        # Workers
        self.workers = []
        # Install signal handlers
        # Signal count
        self.sig_count = 0      
        signal.signal(signal.SIGINT, self.sighandler)
        signal.signal(signal.SIGTERM, self.sighandler)              

    def sighandler(self, signum, stack):
        """ Signal handler """

        if signum in (signal.SIGINT, signal.SIGTERM,):
            log.info('Got signal, stopping...')
            for worker in self.workers:
                worker.stop()
                
            self.sig_count += 1

        if self.sig_count>2:
            log.info('Force Quitting...')
            # Not exited in natural course, force exiting.
            sys.exit(1)

    def subscribe_events(self):
        """ Subscribe to events """

        self.eventr.subscribe('download_complete', self.url_download_complete)
        self.eventr.subscribe('download_error', self.url_download_error)
        self.eventr.subscribe('abort_crawling', self.abort_crawl)
        
    def get(self, timeout=30):
        """ Return the data for crawling """

        try:
            return self.dqueue.get(False, timeout=timeout)
        except Queue.Empty:
            self.empty_count += 1

    def put(self, data):
        """ Push further data to be crawled """

        self.dqueue.put(data)

    def abort_crawl(self, *args):
        """ Stop/abort the crawl """

        # Signal workers to stop
        log.info('Aborting the crawl.')
        for worker in self.workers:
            worker.stop()
        
    def is_empty(self):
        """ Is the work queue empty ? """

        return self.empty_count>2

    def check_already_downloaded(self, url):
        """ Is a URL already downloaded """

        return self.url_bitmap.has_key(url)
    
    def url_download_complete(self, event):
        """ Event callback for notifying download for a URL is done """

        # Mark in bitmap
        url = event.params.get('url')
        log.debug('Making entry for URL',url,'in bitmap...')
        self.url_bitmap[url] = 1

    def url_download_error(self, event):
        """ Event callback for notifying download for a URL in error """

        # Mark in bitmap
        url = event.params.get('url')       
        log.debug('Making entry for URL',url,'in bitmap...')
        self.url_bitmap[url] = 1
        
    def crawl(self):
        """ Do the actual crawling """
        
        # Push the URLs to queue
        for url in self.urls:
            self.dqueue.put(('text/html',url,None))

        # Mark start time
        self.eventr.publish(self, 'crawl_started')

        nworkers = self.config.num_workers
        
        for i in range(nworkers):
            worker = EIIICrawlerQueuedWorker(self.config, self)
            self.workers.append(worker)
            worker.start()
            # Give subsequent workers some time to start so that the other
            # workers fill in some data.
            time.sleep(10*(nworkers - i))           

        # Wait for some time
        time.sleep(10)
        
        for i in range(self.config.num_workers):
            log.info('Joining worker',i+1,'...',)
            worker.join()
            log.info('done.')

        self.eventr.publish(self, 'crawl_ended')        
        log.info('Crawl done.')

        self.stats.publish_stats()


if __name__ == "__main__":
    crawler = EIIICrawler(sys.argv[1:])
    crawler.crawl()
