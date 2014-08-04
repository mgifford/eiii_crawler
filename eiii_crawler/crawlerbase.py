# -- coding: utf-8

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
import os

log = logger.getLogger('eiii_crawler',utils.get_crawl_log() ,console=True)

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

    # Site max depth
    # Maximum depth of a URL relative to site root
    # E.g: http://www.foo.com/a/b/c/d/e/f/g/h/ has a depth of 8
    site_maxdepth = 10
    
    # A tuple of all site inclusive scopes
    all_site_scopes = (site_scope, site_full_scope, site_link_scope,
                       site_full_link_scope, site_exhaustive_scope)

    all_fullsite_scopes = (site_full_scope, site_full_link_scope)
    all_folder_scopes = (folder_scope, folder_link_scope)
    
class CrawlerLimits(object):
    """ Class keeping constants on maximum limits for the
    crawler on various aspects """

    # Map the limits to content-type
    url_limits = {
        'text/html': 8000,
        'application/xhtml+xml':8000,
        'application/xml': 8000,
        'application/pdf': 1000,
        }

    byte_limits = {
        'text/html': 500,
        'application/xhtml':500,
        'application/xml': 500,     
        'application/pdf': 200,
        }
    # Maximum concurrent connections/requests to a site
    site_maxrequests = 20
    # Maximum bytes downloaded from a site in MB
    site_maxbytes = 500
    # Maximum size of single URL from a site in MB
    site_maxurlsize = 20

class CrawlerConfig(object):
    """ Configuration for the Crawler """

    # This class is a Singleton
    __metaclass__ = utils.SingletonMeta
    
    def __init__(self):
        # Site scope
        self.site_scope = CrawlPolicy.site_scope
        # Enable dynamic folder scoping by default
        self.disable_dynamic_scope = False
        # Site depth
        self.site_maxdepth = CrawlPolicy.site_maxdepth

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
        # Store data - if this is set to true, URL data
        # is saved in the local data store. This can be
        # used to avoid network fetchers for URLs which
        # are not modified w.r.t timestamps or Etags.
        self.flag_storedata = True
        # Umbrella config for disabling both below
        self.flag_usecache = True
        # Enable HTTP 304 caching using last-modified ?
        self.flag_use_last_modified = True
        # Enable HTTP 304 caching using etag ?
        self.flag_use_etags = True
        # NOTE that above two would work only if flag_storedata
        # is True, otherwise there is no actual use of these flags.
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
        # Try and process js redirects ?
        self.flag_jsredirects = True
        # Try and pick up additional URLs
        self.flag_supplement_urls = True
        # Support X-Robots-Tag ?
        self.flag_x_robots = True
        # Detect spurious 404s ?
        self.flag_detect_spurious_404 = True
        
        # Network settings - Address of network proxy including port if any
        self.network_proxy = ''

        # Client settings
        self.client_useragent = 'EIII Web Crawler v1.0 - http://www.eiii.eu'
        # Spoofed user-agent
        self.client_spoofuseragent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:26.0) Gecko/20100101 Firefox/26.0'
        # Other headers to send

        # Copied from Firefox standard headers.
        self.client_accept = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        # requests library does automatic gzip decoding, so this is OK.     
        self.client_accept_encoding = 'gzip, deflate'
        self.client_accept_language = 'en-US, en;q=0.5'
        self.client_connection = 'keep-alive'
        self.client_standard_headers = ['Accept','Accept-Encoding','Accept-Language','Connection']

        # Mime-types which we want to deal with
        # All HTML/XML and plain text mime-types only + PDF
        self.client_mimetypes = ['text/html','text/plain','text/xml',
                                 'application/xhtml+xml','application/xml',
                                 'application/pdf']

        # Mime-types we want to deal with (get URLs) but don't want to
        # download - if a mime-type is added here its URLs will be processed
        # till the point of download and will also appear in the URL graph
        # but download will be skipped. For example this could be used to
        # get large sized URLs like PDF documents into the URL graph without
        # actually downloading them.

        # Any URL with a mime-type here will be skipped for download.

        # NOTE: DON'T ADD HTML mime-types here as this means crawl will be
        # incomplete or mostly won't proceed at all!
        
        self.client_fake_mimetypes = ['application/pdf']
        
        # System settings
        self.num_workers = 2
        # Config directory 
        self.configdir = '~/.eiii/crawler'      
        # Store directory for file metadata, defaults to ~/.eiii/crawler/store folder
        self.storedir = os.path.join(self.configdir, 'store')
        # Stats folder
        self.statsdir = os.path.join(self.configdir, 'stats')
        # Additional filtering rules if any in the form of a list like
        # [('+', include_rule_regex), ('-', exclude_rule_regex)] tried
        # in that order.

        # Standard exclude filters
        # 1. */wp-content/*, */wp-includes/*
        # 2. */plugins/*
        # 3. */themes/*
        # 4. */_layouts/*
        # 5. */styles/*
        # 6. */_sources/*
        # 7. */static/*
        # 8. */_static/*
        # 9.  */js/*
        # 10. */stylesheets/*
        self.url_filter =  [('-',  '.*\/wp-content\/.*'),
                            ('-',  '.*\/wp-includes\/.*'),                          
                            ('-', '.*\/plugins\/.*'),
                            ('-', '.*\/themes\/.*'),
                            ('-', '.*\/_layouts\/.*'),
                            ('-', '.*\/styles\/.*'),
                            ('-', '.*\/_sources\/.*'),
                            ('-', '.*\/_static\/.*'),
                            ('-', '.*\/static\/.*'),
                            ('-', '.*\/js\/.*'),
                            ('-', '.*\/stylesheets\/.*')]    
        
        
    def get_real_useragent(self):
        """ Return the effective user-agent string """

        if self.flag_spoofua:
            return self.client_spoofuseragent
        else:
            return self.client_useragent        

    def save(self, filename):
        """ Write the config in JSON format to a file """

        open(filename, 'w').write(json.dumps(self.__dict__, indent=True, sort_keys=True) + '\n')

    def save_default(self):
        """ Save configuration to default location """

        fpath = os.path.expanduser(os.path.join(self.configdir, 'config.json'))
        return self.save(fpath)
    
    @classmethod
    def fromfile(cls, filename):
        """ Create config by loading data from a JSON file """

        config = json.loads(open(filename).read())
        cfg = cls()
        
        # Set value
        cfg.__dict__ = config
        return cfg
        
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

    def __init__(self, publisher, event_name, event_key=None, source=None, message=None,
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
                  'download_complete_fake': 'Published when download of a URL is simulated',                  
                  'download_error': 'Published when a URL fails to download',
                  'download_cache': 'Retrieved a URL from the cache',
                  'url_obtained': 'Published when a URL is obtained from the pipeline for processing',
                  'url_pushed': 'Published when a new URL is pushed to the pipeline for processing',
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
        self.subscribers = collections.defaultdict(set)
        # Dictionary of events published mapped by unique key
        self.unique_events = {}

    def reset(self):
        """ Reset state """
        self.unique_events = {}
        
    def publish(self, publisher, event_name, **kwargs):
        """ API called by the event publisher to announce an event.
        The required arguments for creating the Event object should
        be passed as keyword arguments. The first argument is always
        a handle to the publisher object itself """

        # Not confirming to the list of supported events ? 
        if event_name not in self.__events__:
            log.info('Unrecognized event =>',event_name)
            return False

        event_key = kwargs.get('event_key')
        # print 'EVENT KEY=>',event_key,event_name
        
        # If key is given and event already published, don't publish anymore.
        if event_key != None:
            # Create unique key => (event_key, event_name)
            key = (event_key, event_name)
            if key in self.unique_events:
                log.info("Not publishing event =>", key)
                return False
            else:
                # Add it
                self.unique_events[key] = 1
            
        # Create event object
        event = CrawlerEvent(publisher, event_name, **kwargs)
        # Notify all subscribers
        return self.notify(event)
        
    def notify(self, event):
        """ Notify all subscribers subscribed to an event """
        
        # Find subscribers
        # log.debug('Notifying all subscribers...',event)

        # print self.subscribers
        
        count = 0
        for sub in self.subscribers.get(event.event_name, []):
            # log.info("Calling subscribers for =>",event.event_name, '=>', sub)
            sub(event)
            count += 1

        return count

    def subscribe(self, event_name, method):
        """ Subscribe to an event with the given method """

        self.subscribers[event_name].add(method)

class CrawlerStats(object):
    """ Class keeping crawler statistics such as total URLs downloaded,
    total URLs parsed, total time taken etc """

    # This class is a Singleton
    __metaclass__ = utils.SingletonMeta
    
    def __init__(self):
        self.reset()
        # Subscribe to events
        eventr = CrawlerEventRegistry.getInstance()
        eventr.subscribe('download_complete', self.update_total_urls_downloaded)
        eventr.subscribe('download_cache', self.update_total_urls_cache)       
        eventr.subscribe('download_error', self.update_total_urls_error)
        eventr.subscribe('url_obtained', self.update_total_urls)
        eventr.subscribe('url_filtered', self.update_total_urls_skipped)
        eventr.subscribe('crawl_started', self.mark_start_time)
        eventr.subscribe('crawl_ended', self.mark_end_time)                     
        pass

    def reset(self):
        """ Reset the stats """

        # Number of total URLs crawled
        self.num_urls = 0
        # Number of total URLs downloaded
        self.num_urls_downloaded = 0
        # Number of URLs skipped (due to rules etc)
        self.num_urls_skipped = 0
        # Number of URLs with error
        self.num_urls_error = 0
        # Number of urls not found (404 error)
        self.num_urls_notfound = 0
        # Number of URLs retrieved from cache
        self.num_urls_cache = 0

        # Time
        # Start time-stamp
        self.start_timestamp = ''
        # End time-stamp
        self.end_timestamp = ''
        # Time taken for total crawl
        self.crawl_time = 0
        # Time taken for download
        self.download_time = 0
        # Total sleep time
        self.sleep_time = 0
        
    def update_total_urls(self, event):
        """ Update total number of URLs """

        # NOTE: This also includes duplicates, URLs with errors - everything.
        self.num_urls += 1

    def update_total_urls_cache(self, event):
        """ Update total number of URLs retrieved from cache """

        self.num_urls_cache += 1
        # log.debug('===> Number of URLs from cache <===',self.num_urls_cache)
        
    def update_total_urls_downloaded(self, event):
        """ Update total number of URLs downloaded """

        self.num_urls_downloaded += 1
        # log.debug('===> Number of URLs downloaded <===',self.num_urls_downloaded)

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

        self.start_timestamp = datetime.datetime.now().replace(microsecond=0)

    def mark_end_time(self, event):
        """ Mark end time of crawl """

        self.end_timestamp = datetime.datetime.now().replace(microsecond=0)
        self.crawl_time = str(self.end_timestamp - self.start_timestamp)

    def get_crawl_url_rate(self):
        """ Return crawling rate in terms of # URLs/sec """

        d = datetime.datetime.now()
        delta = (d - self.start_timestamp).total_seconds()
        return 1.0*(self.num_urls_downloaded + self.num_urls_cache)/delta

    def get_num_urls(self):
        """ Get URLs crawled so far """

        return self.num_urls_downloaded + self.num_urls_cache
    
    def publish_stats(self):
        """ Publish crawl stats """

        log.info("Total URLs =>",self.num_urls)
        log.info("Total URLs downloaded =>",self.num_urls_downloaded)
        log.info("Total URLs with error =>",self.num_urls_error)
        log.info("Total URLs from Cache =>",self.num_urls_cache)        
        log.info("Total 404 URLs =>",self.num_urls_notfound)
        log.info("Total URLs skipped =>",self.num_urls_skipped)
        log.info("Crawl start time =>", self.start_timestamp)
        log.info("Crawl end time =>",self.end_timestamp)
        log.info("Time taken for crawl =>",str(self.crawl_time))
        
class CrawlerUrlData(object):
    """ Class representing downloaded data for a URL """

    def __init__(self, url, parent_url, config):
        self.url = url
        self.parent_url = parent_url
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

    def get_url(self):
        """ Return the downloaded URL. This is same as the
        passed URL if there is no modification (such as
        forwarding) """

        return self.url

    def build_headers(self):
        """ Build headers for the request """

        return {}

class CrawlerLimitRules(object):
    """ Class implementing crawler limiting rules with respect
    to maximum limits set if any """

    # This class is a Singleton
    __metaclass__ = utils.SingletonMeta

    def __init__(self, config):
        self.config = config
        # Subscribe to events       
        self.eventr = CrawlerEventRegistry.getInstance()
        self.eventr.subscribe('download_complete', self.check_crawler_limits)

        # Do we need to apply URL limits also for retrievel from cache ?
        # Maybe we should since that also includes a HEAD request for the URL.
        # Anyway for the time being this is enabled.
        self.eventr.subscribe('download_cache', self.check_crawler_limits)      
        self.reset()

    def reset(self):
        """ Reset the state """

        # Map the limits to content-type
        self.url_limits = self.config.url_limits
        self.byte_limits = self.config.byte_limits

        self.url_counts = collections.defaultdict(int)
        self.byte_counts = collections.defaultdict(int)
        
        # Total URL (downloaded) count
        self.num_urls = 0
        # Total bytes downloaded
        self.num_bytes = 0
        
    def update_counts(self, ctype, bytes=0):
        """ Update URL count for the content-type """

        self.url_counts[ctype] += 1
        self.byte_counts[ctype] += bytes

        self.num_bytes += bytes
        self.num_urls += 1
        
        log.debug('===> Updating count for',ctype,' <====', self.url_counts[ctype])
        
    def check_crawler_limits(self, event):
        """ Check whether enough URLs have been downloaded """
        
        # Get URL
        log.debug('Checking crawler limits', threading.currentThread())
        url = event.params.get('url')
        
        if url:
            headers = event.params.get('headers', {})
            # get content type
            ctype = urlhelper.get_content_type(url, headers)
            self.update_counts(ctype, int(event.params.get('content_length', 0)))

            # Get limit for the content-type
            url_limit = self.url_limits.get(ctype)

            log.debug('Limits: ===>',url_limit,self.url_counts.get(ctype, 0),url,'<========')
            if url_limit and self.url_counts.get(ctype, 0)>url_limit:
                log.info('URL limit =>',url_limit,'<= for content-type',ctype,'reached.')
                # Send abort_crawling event
                self.eventr.publish(self, 'abort_crawling',
                                    message='URL limit for content-type "%s" has reached' % ctype)

            # Get limit for the content-type
            byte_limit = self.byte_limits.get(ctype)
            
            if byte_limit and self.byte_counts.get(ctype, 0)>byte_limit*1024*1024:
                log.info('Byte limit for content-type',ctype,'reached.')
                # Send abort_crawling event
                self.eventr.publish(self, 'abort_crawling',
                                    message='Byte limit for content-type "%s" has reached' % ctype)
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
        # Find the 'folder' of the URL
        # E.g: http://www.foo.com/bar/vodka/index.html => http://www.foo.com/bar/vodka/
        self.folder = utils.safedata(urlhelper.get_url_directory(url))
        # Root site
        self.rootsite = urlhelper.get_root_website(self.site)

    def allowed(self, url):
        """ Return whether the URL can be crawled according
        to the configured site scoping rules """

        # print 'Checking scope for',url,'against',self.url
        scope = self.config.site_scope
        # Get the website of the URL
        url_site = urlhelper.get_website(url)

        # Boolean and of values
        ret = True
        
        # If both sites are same
        if self.site == url_site:
            if scope in CrawlPolicy.all_site_scopes:
                # print '\tSame site, all site scope, returning True',url,'=>',self.url
                ret &= True
            elif scope in CrawlPolicy.all_folder_scopes:
                # NOTE - folder_link_scope check is not implemented
                # Only folder scope is done. Folder scope is correct
                # if URL is inside the root folder.
                # E.g: http://www.foo.com/bar/vodka/images/index.html
                # for root folder http://www.foo.com/bar/vodka/
                # print 'URL =>',url
                # print 'FOLDER =>',self.folder
                ret1 = (url.find(self.folder) != -1)
                # Check if both URL and folder has a common prefix
                prefix2 = urlparse.urlparse(os.path.commonprefix((url, self.folder))).path
                prefix1 = urlparse.urlparse(self.folder).path
                # prefix1 and prefix2 shouldn't vary by more than one extra path
                # E.g: /en/locations/qatar/Pages/ vs /en/locations/qatar/
                pieces2, pieces1 = map(lambda x: x.strip('/').split('/'), (prefix2, prefix1))
                ret2 = abs(len(pieces1) - len(pieces2))<=1
                # print 'PIECES BOOLEAN =>',ret2
                # Either ret1 or ret2
                ret &= (ret1 or ret2)
        else:
            # Different site - work out root site
            # If root site is same for example images.foo.com and static.foo.com
            # crawling is allowed if scope is site_full_scope or site_full_link_scope
            url_root_site = urlhelper.get_root_website(url_site)
            if url_root_site == self.rootsite:
                if scope in CrawlPolicy.all_fullsite_scopes:
                    # print '\tSame root site, all full site scope, returning True',url,'=>',self.url                  
                    ret &= True
                else:
                    # print '\tSame root site, but not full-site-scope, returning False',url,'=>',self.url
                    ret &= False
            else:
                # print '\tDifferent root site, returning False',url,'=>',self.url                                
                ret &= False

        # Depth scope
        url_depth = urlhelper.get_depth(url)
        if url_depth > self.config.site_maxdepth:
            ret &= False

        # print '\tDefault value',url,'=>',self.url            
        # default value
        return ret

class CrawlerWorkerBase(threading.Thread):
    """ Base class for EIII web crawler worker. This class does most of the
    work of crawling """

    def __init__(self, config):
        """ Initializer - sets configuration """

        self.config = config
        self.state = 0
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

    def supplement_urls(self, url):
        """ Build any additional URLs related to the input URL """

        return []

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

    def get_url_data_instance(self, url, parent_url=None):
        """ Make an instance of the URL data class
        which fetches the URL """

        return CrawlerUrlData(url, self.config)
    
    def download(self, url, parent_url=None):
        """ Download a URL and return an object which
        represents both its content and the headers. The
        headers are indicated by the 'headers' attribute
        and content by the 'content' attribute of the
        returned object. Returns error code if failed """

        urlobj = self.get_url_data_instance(url, parent_url)
        # Double-dispatch pattern - this is so amazingly useful!
        # Gives you effect of mulitple inheritance with objects.
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

    def get_state(self):
        """ Return the state """
        
        return self.state
    
    def push(self, content_type, url, parent_url, key=None):
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
            # State is 0 - about to get data
            self.state = 0
            data = self.get()
            
            if data==((None,None,None)):
                log.info('No URLs to crawl.')
                break

            # Convert the data to URLs - namely child URL and parent URL and any additional data
            content_type, url, parent_url= self.parse_queue_urls(data)

            # State is 1 - got data, doing work
            self.state = 1
            if self.allowed(url, parent_url, content_type, download=True):
                log.info('Downloading URL',url,'...')
                urlobj = self.download(url, parent_url)
                
                # Data is obtained using the method urlobj.get_data()
                # Headers is obtained using the method urlobj.get_headers()
                url_data = urlobj.get_data()
                headers = urlobj.get_headers()

                # Modified URL if any - this can happen if URL is forwarded
                # etc. Child URLs would need to be constructed against the
                # updated URL not the old one. E.g: https://docs.python.org/library/
                url = urlobj.get_url()

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
                        # Skip empty strings
                        if len(curl.strip())==0: continue
                        # Build full URL
                        full_curl = self.build_url(curl, url)
                        if len(full_curl)==0: continue

                        # Insert this back to the queue
                        content_type = urlhelper.get_content_type(full_curl, headers)
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
                            log.debug("\tPushed new URL =>",curl,"...")
                else:
                    if url_data == None:
                        log.debug("URL data is null =>", url)
                    else:
                        log.debug("URL is disallowed =>", url)

            else:
                log.debug('Skipping URL',url,'...')

            # State is 3, sleeping off
            self.state = 3
            self.sleep()

        # Put state to zero when exiting
        self.state = 0

        log.info('Worker',self,'done.')

    def sleep(self):
        """ Sleep it off """
        
        # Sleep
        if self.flag_randomize_sleep:
            # Randomize 50% on both sides
            time.sleep(random.uniform(self.time_sleeptime, self.time_sleeptime*2))
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
    CrawlerConfig().save_default()
    CrawlerConfig().save('config.json') 
