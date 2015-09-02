""" Classes for implementing a multi-threaded Crawler using
FIFO Queues """

import threading
import uuid
import random
import time
import traceback

from eiii_crawler import utils
from eiii_crawler import urlhelper

from eiii_crawler.crawlerbase import CrawlerUrlData, CrawlerWorkerBase
from eiii_crawler.crawlerevent import CrawlerEventRegistry

# Default logging object
log = utils.get_default_logger()

class ThreadedWorkerBase(threading.Thread, CrawlerWorkerBase):
    """ Base class for a threaded worker """

    def __init__(self, config):
        """ Initializer - sets configuration """

        self.config = config
        self.state = 0
        # Prepare config
        self.prepare_config()
        threading.Thread.__init__(self, None, None, 'ThreadedCrawlerWorker-' + uuid.uuid4().hex)
        CrawlerWorkerBase.__init__(self, config)

    def __getattr__(self, name):
        """ Overloaded getattr method to allow
        access of config variables as local attributes """

        try:
            return self.__dict__[name]
        except KeyError:
            try:
                return getattr(self.config, name)
            except AttributeError:
                raise

    def get_state(self):
        """ Return the state """

        return self.state
    
    def get_url_data_instance(self, url, parent_url=None, content_type='text/html'):
        """ Make an instance of the URL data class
        which fetches the URL """

        return CrawlerUrlData(url, parent_url, content_type, self.config)
    
    def download(self, url, parent_url=None, content_type='text/html'):
        """ Download a URL and return an object which
        represents both its content and the headers. The
        headers are indicated by the 'headers' attribute
        and content by the 'content' attribute of the
        returned object. Returns error code if failed """

        urlobj = self.get_url_data_instance(url, parent_url, content_type)
        # Double-dispatch pattern - this is so amazingly useful!
        # Gives you effect of mulitple inheritance with objects.
        urlobj.download(self)

        return urlobj
    
    def sleep(self):
        """ Sleep it off """
        
        # Sleep
        if self.flag_randomize_sleep:
            # Randomize 50% on both sides
            time.sleep(random.uniform(self.time_sleeptime, self.time_sleeptime*2))
        else:
            time.sleep(self.time_sleeptime)

    def do_crawl(self):
        """ Do the actual crawl. This function provides a pluggable
        crawler workflow structure. The methods called by the crawler
        are pluggable in the sense that sub-classes need to override
        most of them for the actual crawl to work. However the skeleton
        of the workflow is defined by this method.

        A sub-class can implement a new crawl workflow by completely
        overriding this method though it is not suggested.
        """

        eventr = CrawlerEventRegistry.getInstance()

        while self.work_pending() and (not self.should_stop()):
            # State is 0 - about to get data
            self.state = 0
            data = self.get()

            eventr.publish(self, 'heartbeat')
            
            if data==((None,None,None)):
                log.info('No URLs to crawl.')
                break

            # Convert the data to URLs - namely child URL and parent URL and any additional data
            content_type, url, parent_url= self.parse_queue_urls(data)

            # State is 1 - got data, doing work
            self.state = 1
            if self.allowed(url, parent_url, content_type, download=True):
                # Refresh content-type
                content_type = urlhelper.get_content_type(url, {})             
                log.info('Downloading URL',url,'=>',content_type,'...','from parent =>',parent_url)
                
                urlobj = self.download(url, parent_url, content_type)
                
                # Data is obtained using the method urlobj.get_data()
                # Headers is obtained using the method urlobj.get_headers()
                url_data = urlobj.get_data()
                headers = urlobj.get_headers()

                # Modified URL if any - this can happen if URL is forwarded
                # etc. Child URLs would need to be constructed against the
                # updated URL not the old one. E.g: https://docs.python.org/library/
                url = urlobj.get_url()
                # Get updated content-type if any
                content_type = urlobj.get_content_type()
                
                # Make another call to allowed this time with the content and headers - it
                # is up to the child class on how to implement this - for example
                # it can chose to implement content specific rules in another function.
                # In this case the allowed is more applicable to child URLs - for example
                # a META robots NOFOLLOW is parsed at this point.

                if (urlobj.status) and (url_data != None) and \
                       self.allowed(url, parent_url, url_data, content_type, headers, parse=True):

                    # Can proceed further
                    # Parse the data
                    url, child_urls = self.parse(url_data, url)

                    if self.flag_randomize_urls:
                        random.shuffle(child_urls)
                        
                    newurls = []
                    
                    for curl in child_urls:
                        if curl==None: continue
                        # Skip empty strings
                        if len(curl.strip())==0: continue
                        # Build full URL
                        full_curl = self.build_url(curl, url)
                        if len(full_curl)==0: continue

                        # Insert this back to the queue
                        content_type = urlhelper.guess_content_type(full_curl)
                        # Skip if not allowed
                        if self.allowed(full_curl, parent_url=url, content_type=content_type):
                            # log.info(url," => Adding URL",full_curl,"...")
                            newurls.append((content_type, full_curl, url))

                            # Build additional URLs if any
                            other_urls = self.supplement_urls(full_curl)
                            for other_url in other_urls:
                                if self.allowed(other_url, parent_url=url, content_type='text/html'):
                                    # Safely assume HTML for directory URLs
                                    newurls.append(('text/html', other_url, url))                           
                        else:
                            log.debug('Skipping URL',full_curl,'...')

                    # State is 2, did work, pushing new data
                    self.state = 2
                    newurls = list(set(newurls))
                    
                    # Push data into the queue
                    for ctype, curl, purl in newurls:
                        # Key is the child URL itself
                        if self.push(ctype, curl, purl, curl):
                            log.debug("\tPushed new URL =>",curl,'('+ctype+')...')
                            pass
                else:
                    if url_data == None:
                        log.debug("URL data is null =>", url)
                    else:
                        log.debug("URL is disallowed =>", url)

            else:
                # log.debug('Skipping URL',url,'...')
                pass

            # State is 3, sleeping off
            self.state = 3
            self.sleep()

        # Put state to zero when exiting
        self.state = 0

        log.info('Worker',self,'done.')

    def run(self):
        """ Do the actual crawl """

        log.info('Worker',self,'starting...')
        # Defines the "framework" for crawling
        try:
            self.before_crawl()
            self.do_crawl()
            self.after_crawl()
        # Any uncaught exception
        except Exception, e:
            tback = traceback.format_exc()
            log.error("Unhandled exception on worker",self)
            log.error("\tTraceback log => ",tback)
            # raise
            # Raise thread killed event. Maybe this can be listened
            # to by the manager to create a new thread so crawl doesn't
            # get stuck.
            log.info("Worker",self,"died due to unhandled exception",str(e))
            eventr = CrawlerEventRegistry.getInstance()
            eventr.publish(self, 'worker_threw_exception',
                           message='Worker ' + self.name + ' died wih exception ' + str(e),
                           params= self.__dict__)
            
