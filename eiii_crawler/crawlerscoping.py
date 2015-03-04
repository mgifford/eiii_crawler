# -- coding: utf-8
""" Classes which provide scoping rules and enforce limits for the crawler framework """

import utils
import urlhelper
import collections
import datetime
import urlparse
import os

from crawlerevent import CrawlerEventRegistry

# Default logging object
log = utils.get_default_logger()

class CrawlPolicy(object):
    """ Crawl policy w.r.t site root and folders """

    # Crawl any URL in the given site e.g: http://www.foo.com/*
    # But URL redirections out of site would not be allowed
    # E.g: http://foo.com/a => http://bar.com/b is NOT OK   
    site_scope = 'SITE_SCOPE'
    # Crawl any URL in the given site e.g: http://www.foo.com/*
    # Plus URL redirections outside site will be allowed
    # E.g: http://foo.com/a => http://bar.com/b is OK
    site_flexi_scope = 'SITE_FLEXI_SCOPE'
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

    # Maximum time duration of the crawl in minutes - 8 hrs by default
    time_limit = 480
    # Maximum concurrent connections/requests to a site
    # NOTE: Unimplemented
    site_maxrequests = 20
    # Maximum bytes downloaded from a site in MB
    site_maxbytes = 500
    # Maximum size of single URL from a site in MB
    site_maxrequestsize = 5


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

        log.debug('Checking scope for',url,'against',self.url)
        # Fix for issue #420
        # For some websites e.g http://www.vagsoy.kommune.no/ the
        # URL is forwarded to http://vagsoy.kommune.no/. Right now this
        # fails as the rules treat www.foo.com and foo.com as different.
        # We should generally assume that www.foo.com and foo.com
        # are the same - irrespective of any scope.
        if urlhelper.is_www_of(url, self.url):
            log.debug(url,'is a www sister of',self.url,'or the same')
            return True
        
        scope = self.config.site_scope
        # Get the website of the URL
        url_site = urlhelper.get_website(url)

        # Boolean and of values
        ret = True
        
        # If both sites are same
        if self.site == url_site:
            if scope in CrawlPolicy.all_site_scopes:
                log.debug('\tSame site, all site scope, returning True',url,'=>',self.url)
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
                    log.debug('\tSame root site, all full site scope, returning True',url_root_site,'=>',self.url,self.rootsite)
                    ret &= True
                else:
                    log.debug('\tSame root site, but not full-site-scope, returning False',url_root_site,'=>',self.url,self.rootsite)
                    ret &= False
            else:
                log.debug('\tDifferent root site, returning False',url,'=>',self.url,self.rootsite)
                ret &= False

        # Depth scope
        url_depth = urlhelper.get_depth(url)
        if url_depth > self.config.site_maxdepth:
            ret &= False

        # print '\tDefault value',url,'=>',self.url            
        # default value
        return ret

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
        self.eventr.subscribe('crawl_started', self.mark_start_time)
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
        # Maximum time for crawling in seconds
        self.time_limit = self.config.time_limit
        
        self.url_counts = collections.defaultdict(int)
        self.byte_counts = collections.defaultdict(int)
        
        # Total URL (downloaded) count
        self.num_urls = 0
        # Total bytes downloaded
        self.num_bytes = 0
        # Start time-stamp
        self.start_timestamp = 0
        # Duration
        self.duration = 0
        
    def mark_start_time(self, event):
        """ Mark starting time of crawl """

        self.start_timestamp = datetime.datetime.now().replace(microsecond=0)

    def update_time(self):
        """ Update the time taken for crawl """

        tdelta = (datetime.datetime.now() - self.start_timestamp)
        self.duration = tdelta.total_seconds()/60.0
        log.debug("*** Duration of crawl -",self.duration,"minutes ***")

        # If time of crawling exceeded, abort crawling
        if self.duration > self.time_limit:
            log.info('Time duration limit =>',self.time_limit,'<= for crawling reached.')
            # Send abort_crawling event
            self.eventr.publish(self, 'abort_crawling',
                                message='Time limit for crawling exceeded!')      
        
    def update_counts(self, ctype, bytes=0):
        """ Update URL count for the content-type """

        self.url_counts[ctype] += 1
        self.byte_counts[ctype] += bytes

        self.num_bytes += bytes
        self.num_urls += 1
        
        log.debug('===> Updating count for',ctype,' <====', self.url_counts[ctype])
        
    def check_crawler_limits(self, event):
        """ Check whether enough URLs have been downloaded according to
        several limit constraints """
        
        # Get URL
        url = event.params.get('url')

        # Update the total time
        self.update_time()
        
        if url:
            headers = event.params.get('headers', {})
            # get content type
            ctype = urlhelper.get_content_type(url, headers)
            
            self.update_counts(ctype, int(event.params.get('content_length', 0)))

            # Ignore if content-type in fake_mimetypes
            if ctype in self.config.client_fake_mimetypes:
                log.debug('Ignoring limit check for fake mime-type =>', ctype)
                return False

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
    
