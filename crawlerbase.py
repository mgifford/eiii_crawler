""" Base classes for EIII web crawler defining the Crawling API and
the crawling workflow """

import threading
import uuid
import utils
import urlhelper
import random
import urlparse
import time
import collections
import datetime
import json
import logger

log = logger.getMultiLogger('eiii_crawler','crawl.log','crawl.err',console=True)

class CrawlPolicy(object):
    """ Crawl policy w.r.t site root and folders """

    # Crawl any URL in the given site e.g: http://www.foo.com/*
    site_scope = 'SITE_SCOPE'
    # Site and all its subsites E.g: http://www.foo.com and http://server.foo.com etc.
    site_full_scope = 'SITE_FULL_SCOPE'
    # Crawl site scope plus URLs linked outside site at level 1
    site_link_scope = 'SITE_LINK_SCOPE'
    # Site and all its subsites plus URLs linked outside site at level 1
    site_full_link_scope = 'SITE_FULL_LINK_SCOPE'
    # Site and any site linked from it at all levels but not beyond it
    site_exhaustive_scope = 'SITE_EXHAUSTIVE_SCOPE'
    # Crawl only URLs under a given folder e.g: http://www.foo.com/bar/*
    folder_scope = 'FOLDER_SCOPE'
    # Crawl folder scope plus URLs linked outside folder at level 1
    folder_link_scope = 'FOLDER_LINK_SCOPE'

    # A tuple of all site inclusive scopes
    all_site_scopes = (site_scope, site_full_scope, site_link_scope,
                       site_full_link_scope, site_exhaustive_scope)

    all_fullsite_scopes = (site_full_scope, site_full_link_scope) 
    
class CrawlerLimits(object):
    """ Class keeping constants on maximum limits for the
    crawler on various aspects """

    # Maximum number of files fetched from a single site
    site_maxfiles = 10000
    # Max HTML files
    site_maxhtmlfiles = 8000
    # Max PDF files
    site_maxpdffiles = 1000
    # Max other files
    site_maxotherfiles = 1000
    # NOTE that the sum of (site_maxhtmlfiles, site_maxpdffiles, site_maxotherfiles)
    # SHOULD equal value of site_maxfiles.
    
    # Maximum depth of a URL relative to site root
    site_maxdepth = 20
    # Maximum number of URLs to be sampled from a site
    site_maxurls = 10000
    # Maximum concurrent connections/requests to a site
    site_maxrequests = 20
    # Maximum bytes downloaded from a site in MB
    site_maxbytes = 500
    # Maximum size of single file from a site in MB
    site_maxfilesize = 20
    # Flags

# Event management for crawling - this implements a simple decoupled Publisher-Subscriber
# design pattern through a mediator whereby specific events are raised during the crawler workflow.
# Client objects can subscribe to these events by passing a function that should be called
# when the event occurs. The mediator keeps a handle of the functions. When an event occurs
# the object that raises the event does so by calling "publish(publisher, event_name, **kwargs)" on the
# mediator. The mediator creates an Event object from the keyword arguments and notifies all
# the subscribers to that event by invoking their functions they have registerd with the
# event object as argument.

class CrawlerEvent(object):
    """ Class describing a crawler event raised by publishers
    during crawler workflow """

    def __init__(self, publisher, event_name, source=None, message=None,
                 message_type='str',code=0, is_error=False, is_critical=False, 
                 callback=None,params={}):
        # Object that publishes the event. This should
        # never be Null.
        self.publisher = publisher
        # Event published for
        self.event_name = event_name
        # The function or method that raises the event.
        # This can be Null.
        self.source = source
        # Time of publishing
        self.timestamp = datetime.datetime.now()
        # Message string if any. This can be an
        # error message for communicating situations
        # with errors.
        self.message = message
        self.message_type = message_type
        # The message_type is used to 'massage' the message
        # into information
        # Code if any - this can be an error code
        # for communicating situations with errors
        self.code = code
        # Is this an error situation ?
        self.is_error = is_error
        # Indicates an urgent situation
        self.is_critical = is_critical
        # Indicates a callback method (object, not name)
        # that can give more information - this can be Null
        # If not null, the callback should accept the publisher
        # and the same keyword arguments that was sent along
        # with the event as arguments.
        self.callback = callback
        # Dictionary of additional information which is
        # mostly understood only by the subscriber method
        # (Protocol between publisher and subscriber)
        self.params = params
        # ID of the event
        self.id = uuid.uuid4().hex

        # NOTE: The publisher should at least provide the
        # publisher instance itself and a message.
        self._massage()

    def _massage(self):
        """ Massage the message into information """

        try:
            self.data = eval("%s('%s')" % (self.message_type, self.message))
        except Exception, e:
            self.data = ''
            log.error(str(e))

    def __str__(self):
        return 'Event for "%s", published at [%s] - id <%s>' % (self.event_name,
                                                              self.timestamp,
                                                              self.id)

class CrawlerEventRegistry(object):
    """ Event mediator class which allows subscribers to listen to published
    events from event publishers and take actions accordingly """

    # This class is a Singleton
    __metaclass__ = utils.SingletonMeta
    # Dictionary of allowed event names and descriptions
    __events__ = {'download_complete': 'Published when a URL is downloaded successfully',
                  'download_error': 'Published when a URL fails to download',
                  'url_obtained': 'Published when a URL is obtained from the pipeline for processing',
                  'url_parsed': "Published after a URL's data has been parsed for new (child) URLs",
                  'url_filtered': "Published when a URL has been filtered after applying a rule",
                  'crawl_started': "Published when the crawl is started, no events can be published before this event",
                  'crawl_ended': "Published when the crawl ends, no events can be published after this event",
                  'abort_crawling': "Published if the crawl has to be aborted midway"
                  # More to come
                  }
     
    def __init__(self):
        # Dictionary of subscriber objects and the method
        # to invoke mapped against the event name as key
        # A key is mapped to a list of subscribers where each
        # entry in the list is the subscriber method (method
        # object, not name)
        self.subscribers = collections.defaultdict(list)

    def publish(self, publisher, event_name, **kwargs):
        """ API called by the event publisher to announce an event.
        The required arguments for creating the Event object should
        be passed as keyword arguments. The first argument is always
        a handle to the publisher object itself """

        # Not confirming to the list of supported events ? 
        if event_name not in self.__events__:
            log.info('Unrecognized event =>',event_name)
            return False
            
        # Create event object
        event = CrawlerEvent(publisher, event_name, **kwargs)
        # Notify all subscribers
        return self.notify(event)
        
    def notify(self, event):
        """ Notify all subscribers subscribed to an event """
        
        # Find subscribers
        log.debug('Notifying all subscribers...',event)

        count = 0
        for sub in self.subscribers.get(event.event_name, []):
            sub(event)
            count += 1

        return count

    def subscribe(self, event_name, method):
        """ Subscribe to an event with the given method """

        self.subscribers[event_name].append(method)

class CrawlerStats(object):
    """ Class keeping crawler statistics such as total URLs downloaded,
    total URLs parsed, total time taken etc """

    # This class is a Singleton
    __metaclass__ = utils.SingletonMeta
    
    # Stats kept
    # URLs
    # Number of total URLs crawled (not saved)
    num_urls = 0
    # Number of total URLs downloaded
    num_urls_downloaded = 0
    # Number of URLs skipped (due to rules etc)
    num_urls_skipped = 0
    # Number of URLs with error
    num_urls_error = 0
    # Number of urls not found (404 error)
    num_urls_notfound = 0

    # Time
    # Start time-stamp
    start_timestamp = ''
    # End time-stamp
    end_timestamp = ''
    # Time taken for total crawl
    crawl_time = 0
    # Time taken for download
    download_time = 0
    # Total sleep time
    sleep_time = 0

    def __init__(self):
        # Subscribe to events
        eventr = CrawlerEventRegistry.getInstance()
        eventr.subscribe('download_complete', self.update_total_urls_downloaded)
        eventr.subscribe('download_error', self.update_total_urls_error)
        eventr.subscribe('url_obtained', self.update_total_urls)
        eventr.subscribe('url_filtered', self.update_total_urls_skipped)
        eventr.subscribe('crawl_started', self.mark_start_time)
        eventr.subscribe('crawl_ended', self.mark_end_time)                     
        pass

    def update_total_urls(self, event):
        """ Update total number of URLs """

        # NOTE: This also includes duplicates, URLs with errors - everything.
        self.num_urls += 1

    def update_total_urls_downloaded(self, event):
        """ Update total number of URLs downloaded """

        self.num_urls_downloaded += 1
        log.debug('===> Number of URLs downloaded <===',self.num_urls_downloaded)

    def update_total_urls_skipped(self, event):
        """ Update total number of URLs skipped """

        self.num_urls_skipped += 1
        # Skipped URLs have to be added to total URLs
        # since these don't get into the queue
        self.num_urls += 1

    def update_total_urls_error(self, event):
        """ Update total number of URLs that failed to download with error """

        self.num_urls_error += 1        
        if event.code == 404:
            self.num_urls_notfound += 1

    def mark_start_time(self, event):
        """ Mark starting time of crawl """

        self.start_timestamp = datetime.datetime.now()

    def mark_end_time(self, event):
        """ Mark end time of crawl """

        self.end_timestamp = datetime.datetime.now()
        self.crawl_time = self.end_timestamp - self.start_timestamp
        
    def publish_stats(self):
        """ Publish crawl stats """

        log.info("Total URLs =>",self.num_urls)
        log.info("Total URLs downloaded =>",self.num_urls_downloaded)
        log.info("Total URLs with error =>",self.num_urls_error)
        log.info("Total 404 URLs =>",self.num_urls_notfound)
        log.info("Total URLs skipped =>",self.num_urls_skipped)
        log.info("Crawl start time =>",self.start_timestamp)
        log.info("Crawl end time =>",self.end_timestamp)
        log.info("Time taken for crawl =>",str(self.crawl_time))
        
class CrawlerConfig(object):
    """ Configuration for the Crawler """

    # This class is a Singleton
    __metaclass__ = utils.SingletonMeta
    
    def __init__(self):
        # Site scope
        self.site_scope = CrawlPolicy.site_scope

        # Site specific limites
        for attr in CrawlerLimits.__dict__: 
            # Only attributes
            if not attr.startswith('__'):
                setattr(self, attr, getattr(CrawlerLimits, attr))

        # Times
        # Sleep times between crawls
        self.time_sleeptime = 1.0

        # Boolean Flags
        # Randomize sleep ?
        self.flag_randomize_sleep = True
        # Randomize URLs pushed to Queue ?
        self.flag_randomize_urls = False
        # Request data as HTTP compressed ?
        self.flag_httpcompress = True
        # Cache HTTP headers for re-request and supporting
        # last-modified time (304), etags etc
        self.flag_cacheheaders = True
        # Ignore TLDs ? If ignored www.foo.com, www.foo.co.uk, www.foo.org
        # all evaluate to same server so site scope will download
        # from all of these
        self.flag_ignoretlds = False
        # Spoof user-agent ?
        self.flag_spoofua = True
        # Ignore robots txt
        self.flag_ignorerobots = False
        # Obey meta robots txt ?
        self.flag_metarobots = True

        # Network settings - Address of network proxy including port if any
        self.network_proxy = ''

        # Client settings
        self.client_useragent = 'EIII Web Crawler v1.0 - http://www.eiii.eu'
        # Spoofed user-agent
        self.client_spoofuseragent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:26.0) Gecko/20100101 Firefox/26.0'
        # Other headers to send

        # requests library does automatic gzip decoding, so this is OK.

        # Copied from Firefox standard headers.
        self.client_accept = 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        self.client_accept_encoding = 'gzip, deflate'
        self.client_accept_language = 'en-US, en;q=0.5'
        self.client_connection = 'Connection: keep-alive'
        self.client_standard_headers = ['Accept','Accept-Encoding','Accept-Language','Connection']

        # Mime-types which we want to deal with
        # All HTML/XML and plain text mime-types only, no PDF.
        self.client_mimetypes = ['text/html','text/plain','text/xml','application/xhtml+xml','application/xml']
        
        # System settings
        self.num_workers = 2

    def get_real_useragent(self):
        """ Return the effective user-agent string """

        if self.flag_spoofua:
            return self.client_spoofuseragent
        else:
            return self.client_useragent        

    def save(self, filename):
        """ Write the config in JSON format to a file """

        open(filename, 'w').write(json.dumps(self.__dict__, indent=True, sort_keys=True) + '\n')

    @classmethod
    def fromfile(cls, filename):
        """ Create config by loading data from a JSON file """

        config = json.loads(open(filename).read())
        cfg = cls()
        
        # Set value
        cfg.__dict__ = config
        return cfg
        
class CrawlerUrlData(object):
    """ Class representing downloaded data for a URL """

    def __init__(self, url, config):
        self.url = url
        # Useful stuff
        self.useragent = config.get_real_useragent()
        self.config = config

    def download(self, crawler):
        """ Download the data """

        raise NotImplementedError

    def get_headers(self):
        """ Return the headers for the downloaded URL
        as a dictionary """

        return {}

    def get_data(self):
        """ Return the data for the downloaded URL
        as a string """

        return ''

    def build_headers(self):
        """ Build headers for the request """

        return {}

class CrawlerLimitRules(object):
    """ Class implementing crawler limiting rules with respect
    to maximum limits set if any """

    def __init__(self, config):
        self.config = config
        # Map the limits to content-type
        self.limits = {'text/html': self.config.site_maxhtmlfiles,
                       'application/pdf': self.config.site_maxpdffiles}
        self.url_counts = collections.defaultdict(int)
        # Subscribe to events
        self.eventr = CrawlerEventRegistry.getInstance()
        self.eventr.subscribe('download_complete', self.check_crawler_limits)

    def update_counts(self, ctype):
        """ Update URL count for the content-type """

        self.url_counts[ctype] += 1
        log.debug('===> Updating count for',ctype,' <====', self.url_counts[ctype])
        
    def check_crawler_limits(self, event):
        """ Check whether enough URLs have been downloaded """
        
        # Get URL
        log.debug('Checking crawler limits')
        url = event.params.get('url')
        if url:
            headers = event.params.get('headers', {})
            # get content type
            ctype = urlhelper.get_content_type(url, headers)
            self.update_counts(ctype)
            
            # Get limit for the content-type
            url_limit = self.limits.get(ctype)

            log.debug('Limits: ===>',url_limit,self.url_counts.get(ctype, 0),'<========')
            if url_limit and self.url_counts.get(ctype, 0)>url_limit:
                log.info('URL limit for content-type',ctype,'reached.')
                # Send abort_crawling event
                self.eventr.publish(self, 'abort_crawling',
                                    message='URL limit for content-type "%s" has reached' % ctype)
        else:
            pass
    
class CrawlerScopingRules(object):
    """ Class implementing crawler scoping rules with respect
    to site and URL """

    def __init__(self, config, url):
        self.config = config
        self.url = url
        # Find the site without the scheme
        self.site = urlhelper.get_website(url)
        # Root site
        self.rootsite = urlhelper.get_root_website(self.site)

    def allowed(self, url):
        """ Return whether the URL can be crawled according
        to the configured site scoping rules """

        # print 'Checking scope for',url,'against',self.url
        scope = self.config.site_scope
        # Get the website of the URL
        url_site = urlhelper.get_website(url)

        # If both sites are same
        if self.site == url_site:
            if scope in CrawlPolicy.all_site_scopes:
                # print 'Same site, all site scope, returning True'
                return True
        else:
            # Different site - work out root site
            # If root site is same for example images.foo.com and static.foo.com
            # crawling is allowed if scope is site_full_scope or site_full_link_scope
            url_root_site = urlhelper.get_root_website(url_site)
            if url_root_site == self.rootsite:
                if scope in CrawlPolicy.all_fullsite_scopes:
                    # print 'Same root site, all full site scope, returning True'                  
                    return True
            else:
                # print 'Different root site, returning False'                                
                return False

        # Folder scopes and external site scopes
        # to be worked out later.
        return True
    
            
class CrawlerWorkerBase(threading.Thread):
    """ Base class for EIII web crawler worker. This class does most of the
    work of crawling """

    def __init__(self, config):
        """ Initializer - sets configuration """

        self.config = config
        # Prepare config
        self.prepare_config()
        threading.Thread.__init__(self, None, None, 'CrawlerWorker-' + uuid.uuid4().hex)

    def prepare_config(self):
        """ Prepare configuration """

        pass
        
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
        
    def parse(self, content, url):
        """ Parse web-page content and return an iterator on child URLs """
        raise NotImplementedError

    def build_url(self, child_url, parent_url):
        """ Build the complete URL for child URL using the parent URL """
        raise NotImplementedError

    def parse_queue_urls(self, data):
        """ Given the queue URL data return a 3 tuple of content-type, URL and
        parent URL """

        raise NotImplementedError
                     
    def allowed(self, url, parent_url=None, content=None, content_type='text/html', headers={}):
        """ Return True if the crawl rules permit the URL
        to be crawled or False otherwise. By defaul this
        method also processes robots.txt rules if any found
        for the site """

        # Parsing of robots.txt is implicit in this method
        raise NotImplementedError

    def get_url_data_instance(self, url):
        """ Make an instance of the URL data class
        which fetches the URL """

        return CrawlerUrlData(url, self.config)
    
    def download(self, url):
        """ Download a URL and return an object which
        represents both its content and the headers. The
        headers are indicated by the 'headers' attribute
        and content by the 'content' attribute of the
        returned object. Returns error code if failed """

        urlobj = self.get_url_data_instance(url)
        urlobj.download(self)

        return urlobj

    def before_crawl(self):
        """ Actions to execute before crawl starts """

        pass

    def work_pending(self):
        """ Is work (URLs) pending to be done (crawled) ? """

        return True

    def should_stop(self):
        """ Should stop now ? """

        return False

    def stop(self):
        """ Forcefully stop the crawl """

        raise NotImplementedError
    
    def get(self, timeout=30):
        """ Return data (URLs) to be crawled """

        raise NotImplementedError

    def push(self, data):
        """ Push new data back """

        raise NotImplementedError

    def do_crawl(self):
        """ Do the actual crawl. This function provides a pluggable
        crawler workflow structure. The methods called by the crawler
        are pluggable in the sense that sub-classes need to override
        most of them for the actual crawl to work. However the skeleton
        of the workflow is defined by this method.

        A sub-class can implement a new crawl workflow by completely
        overriding this method though it is not suggested.
        """

        while self.work_pending() and (not self.should_stop()):
            data = self.get()
            if not data:
                log.info('No URLs to crawl.')
                break

            # Convert the data to URLs - namely child URL and parent URL and any additional data
            content_type, url, parent_url= self.parse_queue_urls(data)

            if self.allowed(url, parent_url, content_type):
                log.info('Downloading URL',url,'...')
                urlobj = self.download(url)
                
                # Data is obtained using the method urlobj.get_data()
                # Headers is obtained using the method urlobj.get_headers()
                url_data = urlobj.get_data()
                headers = urlobj.get_headers()

                # Make another call to allowed this time with the content and headers - it
                # is up to the child class on how to implement this - for example
                # it can chose to implement content specific rules in another function.
                # In this case the allowed is more applicable to child URLs - for example
                # a META robots NOFOLLOW is parsed at this point.

                if (url_data != None) and self.allowed(url, parent_url, url_data, content_type, headers):
                    # Can proceed further
                    # Parse the data
                    child_urls = self.parse(url_data, url)

                    if self.flag_randomize_urls:
                        random.shuffle(child_urls)
                        
                    newurls = []
                    for curl in child_urls:
                        # Skip empty strings
                        if len(curl.strip())==0: continue
                        # Build full URL
                        full_curl = self.build_url(curl, url)
                        # Insert this back to the queue
                        content_type = urlhelper.get_content_type(full_curl, headers)
                        # Skip if not allowed
                        if self.allowed(full_curl, parent_url=url, content_type=content_type):
                            newurls.append((content_type, full_curl, url))
                        else:
                            log.info('Skipping URL',full_curl,'...')
                        
                    # Push data into the queue
                    for newurl in newurls:
                        self.push(newurl)
                else:
                    log.debug('No data for URL or content-rules do not allow indexing',url,'...')

            else:
                log.info('Skipping URL',url,'...')

            self.sleep()

        log.info('Worker',self,'done.')

    def sleep(self):
        """ Sleep it off """
        
        # Sleep
        if self.flag_randomize_sleep:
            # Randomize 50% on both sides
            time.sleep(random.uniform(self.time_sleeptime*0.5, self.time_sleeptime*1.5))
        else:
            time.sleep(self.time_sleeptime)

    def after_crawl(self):
        """ Actions to execute after the crawl """

        pass
    
    def run(self):
        """ Do the actual crawl """

        log.info('Worker',self,'starting...')
        # Defines the "framework" for crawling
        self.before_crawl()
        self.do_crawl()
        self.after_crawl()
    
if __name__ == "__main__":
    CrawlerConfig().save('config.json')
