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
    """ Class keeping constants on maximum limits for the crawler
    on various aspects """

    # Maximum number of files fetched from a single site
    site_maxfiles = 10000
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
        # print 'Params dict =>',params, event_name
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
            print e

    def __str__(self):
        return 'Event for "%s", published at [%s] - id <%s>' % (self.event_name,
                                                              self.timestamp,
                                                              self.id)

class CrawlerEventRegistry(object):
    """ Event mediator class which allows subscribers to listen to published
    events from event publishers and take actions accordingly """

    # This class is a Singleton
    __metaclass__ = utils.SingletonMeta
    
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

        # Create event object
        event = CrawlerEvent(publisher, event_name, **kwargs)
        # Notify all subscribers
        self.notify(event)

    def notify(self, event):
        """ Notify all subscribers subscribed to an event """
        
        # Find subscribers
        print 'Notifying all subscribers...',event
        for sub in self.subscribers.get(event.event_name, []):
            sub(event)

    def subscribe(self, event_name, method):
        """ Subscribe to an event with the given method """

        self.subscribers[event_name].append(method)

class CrawlerStats(object):
    """ Class keeping crawler statistics such as total URLs downloaded,
    total URLs parsed, total time taken etc """

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

        print "Total URLs =>",self.num_urls
        print "Total URLs downloaded =>",self.num_urls_downloaded
        print "Total URLs with error =>",self.num_urls_error
        print "Total 404 URLs =>",self.num_urls_notfound
        print "Total URLs skipped =>",self.num_urls_skipped
        print "Crawl start time =>",self.start_timestamp
        print "Crawl end time =>",self.end_timestamp
        print "Time taken for crawl =>",str(self.crawl_time)
        
class CrawlerConfig(dict):
    """ Configuration for the Crawler """

    # This class is a Singleton
    __metaclass__ = utils.SingletonMeta
    
    def __init__(self):
        # Site scope
        self.site_scope = CrawlPolicy.site_scope

        # Site specific limites
        for attr in ('maxfiles','maxdepth','maxurls','maxrequests','maxbytes', 'maxfilesize'):
            fullattr = 'site_' + attr
            setattr(self, fullattr, getattr(CrawlerLimits, fullattr))

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
        self.client_spoofuseragent = 'Mozilla/5.0'

    def get_real_useragent(self):
        """ Return the effective user-agent string """

        if self.flag_spoofua:
            return self.client_spoofuseragent
        else:
            return self.client_useragent        

class CrawlerUrlData(object):
    """ Class representing downloaded data for a URL """

    def __init__(self, url, config):
        self.url = url
        # Useful stuff
        self.useragent = config.get_real_useragent()

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

    def get(self, timeout=30):
        """ Return data (URLs) to be crawled """

        raise NotImplementedError

    def push(self, data):
        """ Push new data back """

        raise NotImplementedError

    def _guess_content_type(self, url):
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
        
    def get_content_type(self, url, headers):
        """ Given a URL and its headers find the content-type """

        # If there is a content-type field in the headers
        # return using that
        try:
            return headers['content-type']
        except KeyError:
            return self._guess_content_type(url)
        
    def do_crawl(self):
        """ Do the actual crawl """

        while self.work_pending() and (not self.should_stop()):
            data = self.get()
            if not data:
                print 'No URLs to crawl.'
                break

            # Convert the data to URLs - namely child URL and parent URL and any additional data
            content_type, url, parent_url= self.parse_queue_urls(data)

            if self.allowed(url, parent_url, content_type):
                print 'Downloading URL',url,'...'
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
                        content_type = self.get_content_type(full_curl, headers)
                        # Skip if not allowed
                        if self.allowed(full_curl, parent_url=url, content_type=content_type):
                            newurls.append((content_type, full_curl, url))
                        else:
                            print 'Skipping URL',full_curl,'...'
                        
                    # Push data into the queue
                    for newurl in newurls:
                        self.push(newurl)
                else:
                    print 'No data for URL or content-rules do not allow indexing',url,'...'

            else:
                print 'Skipping URL',url,'...'

            self.sleep()

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

        # Defines the "framework" for crawling
        self.before_crawl()
        self.do_crawl()
        self.after_crawl()
    
if __name__ == "__main__":
    t = CrawlerWorkerBase(CrawlerConfig())
    t.start()
