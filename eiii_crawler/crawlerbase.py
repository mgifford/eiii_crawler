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
import os

from eiii_crawler.crawlerscoping import CrawlPolicy, CrawlerLimits
from eiii_crawler.crawlerevent import CrawlerEventRegistry

class ConfigOutdatedException(Exception):
    """ Exception class indicating the config file is out-dated """
    pass

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
        # Do SSL certificate validations for HTTPS requests ?
        self.flag_ssl_validate = False
        
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


        # Update Aug 11 2014 - fake mimetypes URLs are fetched using
        # a head request to get updated URL. For example this could be used to
        # get large sized URLs like PDF documents into the URL graph without
        # actually downloading them.
        
        # Any URL with a mime-type here would be fetched using a head
        # request and not using a regular (GET) request

        # NOTE: DON'T ADD HTML mime-types here as this means crawl will be
        # incomplete or mostly won't proceed at all!
        self.client_fake_mimetypes = ['application/pdf']

        # Cheats - Mime-types we want to deal with (get URLs) but don't want to
        # download - if a mime-type is added here its URLs will be processed
        # till the point of download and will also appear in the URL graph
        # but download will be skipped. URLs for these mime-types will be
        # completely skipped in download.
        self.client_cheat_mimetypes = []
        
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
        # 11. */login/*
        # 12. */_login/*
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
                            ('-', '.*\/stylesheets\/.*'),
                            ('-', '.*\/login\/.*'),
                            ('-', '.*\/_login\/.*')]

        # Crawler log options
        # 3 options
        # 1. task - Uses task id to create log files
        # 2. site - Uses site name to create log files
        # 3. site_task - Mix of task id + site name (default)
        # 4. site_folder - Sub-folders with site names and log files with task ids
        self.logfile_theme = 'site_task'

        # List of enabled plugins
        self.plugins = ['circuitbreaker']

    def update(self, configdict):
        """ Update configuration from another dictionary """

        # For values other than dictionaries - simply update
        # For values which are dictionaries - merge.
        for k,v in configdict.items():
            if type(v) is not dict:
                self.__dict__[k] = v
            else:
                val = self.__dict__.get(k, {})
                # Merge it
                val.update(v)
                self.__dict__[k] = val
        
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

        # Check if all keys are present, otherwise raise config out of date
        # error!
        for key in cfg.__dict__:
            # Omit private keys, i.e those starting with an underscore
            if key.startswith('_'): continue
            
            if key not in config:
                raise ConfigOutdatedException,"Missing key '%s' => Config file is out-of-date! Regenerate config file by running crawlerbase.py as a script." % key
        
        # Set value
        cfg.__dict__ = config
        return cfg
        
class CrawlerUrlData(object):
    """ Class representing downloaded data for a URL """

    def __init__(self, url, parent_url, content_type,  config):
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

class CrawlerWorkerBase(object):
    """ Base class for EIII web crawler worker. This class does most of the
    work of crawling """

    def __init__(self, config):
        """ Initializer - sets configuration """

        pass

    def prepare_config(self):
        """ Prepare configuration """
        pass
        
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
        """ Make an instance of the URL data class which fetches the URL """
    
        raise NotImplementedError
    
    def download(self, url, parent_url=None):
        """ Download a URL and return an object which
        represents both its content and the headers. The
        headers are indicated by the 'headers' attribute
        and content by the 'content' attribute of the
        returned object. Returns error code if failed """

        raise NotImplementedError

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

        pass

    
    def push(self, content_type, url, parent_url, key=None):
        """ Push new data back """

        raise NotImplementedError

    def do_crawl(self):
        """ Do the actual crawl. """

        raise NotImplementedError

    def sleep(self):
        """ Sleep it off """
        
        raise NotImplementedError

    def after_crawl(self):
        """ Actions to execute after the crawl """
        pass
    
    
if __name__ == "__main__":
    CrawlerConfig().save_default()
    print 'Crawler config regenerated and saved to default location.'
    CrawlerConfig().save('config.json')
    print 'Crawler config regenerated and saved to config.json.' 
