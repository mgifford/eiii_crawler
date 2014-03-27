""" Base classes for EIII web crawler defining the Crawling API """

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
    
class CrawlerLimits(object):
    """ Class describing limits of the crawler """

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
    
    
class CrawlerConfig(dict):
    """ Configuration for the Crawler """

    def __init__(self):
        # Site scope
        self.site_scope = CrawlPolicy.site_full_scope

        # Site specific limites
        for attr in ('maxfiles','maxdepth','maxurls','maxrequests','maxbytes', 'maxfilesize'):
            fullattr = 'site_' + attr
            setattr(self, fullattr, getattr(CrawlerLimits, fullattr))

        # Times
        # Sleep times between crawls
        self.time_sleeptime = 1.0

        # Boolean Flags
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

        # Network settings - Address of network proxy including port if any
        self.network_proxy = ''

        # Client settings
        self.client_useragent = 'EIII Web Crawler v1.0 - http://www.eiii.eu'
        # Spoofed user-agent
        self.client_spoofuseragent = 'Mozilla/5.0'
    
class CrawlerBase(object):
    """ Base class for EIII web crawler defining the crawler API """

    def __init__(self, config):
        """ Initializer - sets configuration """

        self.config = config

    def parse(self, content):
        """ Parse web-page content and return an iterator on child URLs """
        raise NotImplementedError

    def build_url(self, child_url, parent_url):
        """ Build the complete URL for child URL using the parent URL """
        raise NotImplementedError

    def allowed(self, url, ignorerobots=False):
        """ Return True if the crawl rules permit the URL
        to be crawled or False otherwise. By defaul this
        method also processes robots.txt rules if any found
        for the site """

        # Parsing of robots.txt is implicit in this method
        raise NotImplementedError

    def download(self, url):
        """ Download a URL and return an object which
        represents both its content and the headers. The
        headers are indicated by the 'headers' attribute
        and content by the 'content' attribute of the
        returned object. Returns error code if failed """

        raise NotImplementedError
    
    
    
    
