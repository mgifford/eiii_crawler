# -- coding: utf-8

""" Implementation of EIII crawler """

import crawlerbase
from crawlerbase import CrawlerEventRegistry, CrawlPolicy
import sys, os
import Queue
import requests
import urlhelper
import robocop
import urllib
import urlparse
import signal
import binascii
import re
import time
import logger
import utils
import argparse
import hashlib
import zlib
import collections
import sgmllib
import uuid
import sqlite3
import json
import cPickle
import socket
import js.jsparser as jsparser

log = logger.getLogger('eiii_crawler','crawl.log',console=True)

# Default timeout set to 15s
socket.setdefaulttimeout(15)

__version__ = '1.0a'
__author__ = 'Anand B Pillai'
__maintainer__ = 'Anand B Pillai'

class EIIICrawlerUrlData(crawlerbase.CrawlerUrlData):
    """ Class representing downloaded data for a URL.
    The data is downloaded using the TingtunUtils fetcher """

    def __init__(self, url, parent_url, config):
        super(EIIICrawlerUrlData, self).__init__(url, parent_url, config)
        self.orig_url = self.url
        self.headers = {}
        self.content = ''
        self.content_length = 0
        # Download status
        # True -> Success
        # False -> Failed
        self.status = False
        self.content_type = 'text/html'
        
    def get_url_store_paths(self):
        """ Return a 3-tuple of paths to the URL data and header files
        and their directory """

        # Let us write against original URL
        urlhash = hashlib.md5(self.orig_url).hexdigest()
        # First two bytes for folder, next two for file
        folder, sub_folder, fname = urlhash[:2], urlhash[2:4], urlhash[4:]
        # Folder is inside 'store' directory
        dirpath = os.path.expanduser(os.path.join(self.config.storedir, folder, sub_folder))
        # Data file
        fpath = os.path.expanduser(os.path.join(dirpath, fname))
        # Header file
        fhdr = fpath + '.hdr'

        return (fpath, fhdr, dirpath)
        
    def write_headers_and_data(self):
        """ Save the headers and data for the URL to the local store """

        if self.config.flag_storedata:
            fpath, fhdr, dirpath = self.get_url_store_paths()
            # Write data to fpath
            try:
                with utils.ignore(): os.makedirs(dirpath)
                if self.content:
                    open(fpath, 'wb').write(zlib.compress(self.content))
                    log.info('Wrote URL content to',fpath,'for URL',self.url)
                if self.headers:
                    open(fhdr, 'wb').write(zlib.compress(str(dict(self.headers))))
                    log.info('Wrote URL headers to',fhdr,'for URL',self.url)                    
            except Exception, e:
                # raise
                log.error("Error in writing URL data for URL",self.url)
                log.error("\t",str(e))

    def make_head_request(self, headers):
        """ Make a head request with header values (if-modified-since and/or etag).
        Return True if data is up-to-date and False otherwise. """

        lmt, etag = headers.get('last-modified'), headers.get('etag')
        if lmt != None or etag != None:
            req_header = {}
            if lmt != None and self.config.flag_use_last_modified:
                req_header['if-modified-since'] = lmt
            if etag != None and self.config.flag_use_etags:
                req_header['if-none-match'] = etag

            try:
                fhead = urlhelper.head_url(self.url, headers=req_header)
                # Status code is 304 ?
                if fhead.status_code == 304:
                    return True
            except urlhelper.FetchUrlException, e:
                pass

        # No lmt or etag or URL is not uptodate
        return False
        
    def get_headers_and_data(self):
        """ Try and retrieve data and headers from the cache. If cache is
        up-to-date, this sets the values and returns True. If cache is out-dated,
        returns False """

        if self.config.flag_usecache:
            fpath, fhdr, dirpath = self.get_url_store_paths()

            if os.path.isfile(fpath) and os.path.isfile(fhdr):
                try:
                    content = zlib.decompress(open(fpath).read())
                    headers = eval(zlib.decompress(open(fhdr).read()))

                    if self.make_head_request(headers):
                        log.info(self.url, "==> URL is up-to-date, returning data from cache")

                        self.content = content
                        self.headers = headers

                        eventr = CrawlerEventRegistry.getInstance()                 
                        # Raise the event for retrieving URL from cache
                        eventr.publish(self, 'download_cache',
                                       message='URL has been retrieved from cache',
                                       code=304,
                                       params=self.__dict__)                    

                        return True
                except Exception, e:
                    log.error("Error in getting URL headers & data for URL",self.url)
                    log.error("\t",str(e))

        return False
        
    def build_headers(self):
        """ Build headers for the request """

        # User-agent is always sent
        headers = {'user-agent': self.useragent}
        for hdr in self.config.client_standard_headers:
            val = getattr(self.config, 'client_' + hdr.lower().replace('-','_'))
            headers[hdr] = val

        return headers

    def download(self, crawler, parent_url=None):
        """ Overloaded download method """

        eventr = CrawlerEventRegistry.getInstance()

        index, follow = True, True
        
        if self.get_headers_and_data():
            self.status = True          
            # Obtained from cache
            return True
        
        try:
            log.debug("Waiting for URL",self.url,"...")
            freq = urlhelper.get_url(self.url, headers = self.build_headers())
            log.debug("Downloaded URL",self.url,"...")          

            self.content = freq.content
            self.headers = freq.headers

            # Is the URL modified ? if so set it 
            if self.url != freq.url:
                self.url = freq.url
                log.extra("URL updated to",self.url)
            
            # Add content-length also for downloaded content
            self.content_length = max(len(self.content),
                                      self.headers.get('content-length',0))

            self.content_type =  urlhelper.get_content_type(self.url, self.headers)
            # requests does not raise an exception for 404 URLs instead
            # it is wrapped into the status code

            # Accept all 2xx status codes for time being
            # No special processing for other status codes
            # apart from 200.

            # NOTE: requests library handles 301, 302 redirections
            # very well so we dont need to worry about those codes.
            
            # Detect pages that give 2xx code WRONGLY when actual
            # code is 404.
            status_code = freq.status_code
            if self.config.flag_detect_spurious_404:
                status_code = urlhelper.check_spurious_404(self.headers, self.content, status_code)
            
            if status_code in range(200, 300):
                self.status = True
                eventr.publish(self, 'download_complete',
                               message='URL has been downloaded successfully',
                               code=200,
                               params=self.__dict__)
            else:
                log.error("Error downloading URL =>",self.url,"status code is ", status_code)
                eventr.publish(self, 'download_error',
                               message='URL has not been downloaded successfully',
                               code=freq.status_code,
                               params=self.__dict__)

            self.write_headers_and_data()
        except urlhelper.FetchUrlException, e:
            log.error('Error downloading',self.url,'=>',str(e))
            # FIXME: Parse HTTP error string and find out the
            # proper code to put here if HTTPError.
            eventr.publish(self, 'download_error',
                           message=str(e),
                           is_error = True,
                           code=0,
                           params=self.__dict__)

        return True

            
    def get_data(self):
        """ Return the data """
        return self.content

    def get_headers(self):
        """ Return the headers """
        return self.headers

    def get_url(self):
        """ Return the downloaded URL. This is same as the
        passed URL if there is no modification (such as
        forwarding) """

        return self.url 
    
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
        super(EIIICrawlerQueuedWorker,  self).__init__(config)

    def prepare_config(self):
        """ Prepare configuration """

        pass
    
    def get(self, timeout=30):
        """ Get the data to crawl """

        data = self.manager.get()
        log.debug("\tGot data =>", data)
        return data

    def push(self, content_type, url, parent_url=None, key=None):
        """ Push new data to crawl """

        return self.manager.put(content_type, url, parent_url, key)
    
    def parse_queue_urls(self, data):
        """ Parse the URL data from the queue and return a 3-tuple
        of content-type, URL, parent-URL """

        # In this case we return directly as the data that is
        # pushed exactly match this structure
        return data

    def get_url_data_instance(self, url, parent_url=None):
        """ Make an instance of the URL data class
        which fetches the URL """

        return EIIICrawlerUrlData(url, parent_url, self.config)

    def build_url(self, child_url, parent_url):
        """ Build the complete URL for child URL using the parent URL """

        builder = urlhelper.URLBuilder(child_url, parent_url)
        url = builder.build()

        if (url != None) and len(url)>0:
            # Each data is a URL, so raise the event
            # 'obtained_url' here - we may not make use of
            # all these URLs of course
            try:
                self.eventr.publish(self, 'url_obtained',
                                    params=locals())
            except ValueError:
                pass

        return url
        

    def supplement_urls(self, url):
        """ Build any additional URLs related to the input URL """

        # Get parent directory URLs - for example,
        # http://www.foo.com/bar/vodka/ for
        # http://www.foo.com/bar/vodka/beer.html
        # NOTE that this might cause scope issues if
        # crawl is in folder scope.

        if self.config.flag_supplement_urls:
            dir_url = urlhelper.get_url_parent_directory(url)
            if dir_url:
                return [dir_url]

        return []
        
    def parse(self, data, url):
        """ Parse the URL data and return an iterator over child URLs """

        urls, redirect = [], False
        
        # First parse with JS parser
        if self.config.flag_jsredirects:
            try:
                jsp = jsparser.JSParser()
                jsp.parse(data)
                # Check if location changed
                if jsp.location_changed:
                    jsurl = jsp.getLocation().href
                    urls.append(jsurl)
                    log.info("Javascript redirection to =>", jsurl)
                    redirect = True
                    
            except jsparser.JSParserException, e:
                log.error("JS Parser exception => ", e)
            

        # If JS redirect don't bother to parse with
        # HTML parser
        if redirect:
            return (url, urls)
        
        parser = urlhelper.URLLister()

        try:
            log.info("Parsing URL", url)
            parser.feed(data)
            parser.close()

            self.eventr.publish(self, 'url_parsed',
                                params=locals())
            
        except sgmllib.SGMLParseError, e:
            log.error("Error parsing data for URL =>",url)

        # Do we have a redirect ?
        if parser.redirect:
            # Then only the follow URL
            urls = [parser.follow_url]
            log.info("Page redirected to =>",urls[0])                        
        else:
            urls = list(set(parser.urls))

        # Has the base URL changed ?
        if parser.base_changed:
            url = parser.source_url
            
        return (url, urls)

    def should_stop(self):
        """ Should stop now ? """

        return self.stop_now

    def stop(self):
        """ Forcefully stop the crawl """

        log.info('Worker',self,'stopping...')
        self.stop_now = True
        
    def work_pending(self):
        """ Whether crawl work is still pending """

        # Is the queue empty ?
        log.debug('Checking work pending...',)
        return self.manager.work_pending()

    def allowed(self, url, parent_url=None, content=None, content_type='text/html', headers={},
                parse=False, download=False):
        """ Is fetching or parsing of URL allowed ? """        

        # NOTE: This is a wrapper over the actual function _allowed which does all
        # the work. This is to allow publication of events after capturing the return
        # value of the method.
        result = self._allowed(url, parent_url=parent_url, content=content,
                               content_type=content_type, headers=headers,
                               parse=parse, download=download)

        if not result:
            # Filtered
            self.eventr.publish(self, 'url_filtered',
                                params=locals())

        return result
        
    def _allowed(self, url, parent_url=None, content=None, content_type='text/html', headers={},
                 parse=False, download=False):
        """ Is fetching of URL allowed ? - Actual Implementation """                

        # import pdb; pdb.set_trace()
        # Is already downloaded ? Then skip right away
        # NOTE - Do this only for child URLs!
        if (parent_url != None) and (not parse) and self.manager.check_already_downloaded(url):
            log.debug(url,'=> already downloaded')
            return False

        # print 'Checking allowd for URL',url
        # Skip mime-types we don't want to deal with based on URL extensions
        content_type = urlhelper.guess_content_type(url)

        if content_type not in self.config.client_mimetypes:
            log.debug('Skipping URL',url,'as content-type',content_type,'is not valid.')
            return False

        # Part of client mime-types, check if part of fake mime-types
        elif content_type in self.config.client_fake_mimetypes and download:
            log.debug('Skipping URL',url,'as',content_type,'is part of fake mime-types (no download)')
            # Simulate download event for this URL so it gets added to URL graph
            # Publish fake download complete event          
            self.eventr.publish(self, 'download_complete_fake',
                                message='URL has been downloaded fakily',
                                code=200,
                                params=locals())            
            
            return False
        
        # If URL include rules are given - the scenario is most likely
        # if these are filtered by some of the other rules - so we should
        # apply them first.
        if any([re.match(rule, url) for rule in self.config.url_include_rules]):
            log.extra('Allowing URL',url,'due to specific inclusion rule.')         
            return True

        # Apply exclude rules next
        if any([re.match(rule, url) for rule in self.config.url_exclude_rules]):
            log.extra('Disallowing URL',url,'due to specific exclusion rule.')                      
            return False

        # Scoping rules
        if parent_url != None:
            scoper = crawlerbase.CrawlerScopingRules(self.config, parent_url)
            # import pdb; pdb.set_trace()
            
            # Proceed further - do site scoping rules
            if not scoper.allowed(url):
                log.debug('Scoping rules does not allow URL=>',url)
                return False
                        
        if (content != None) or len(headers):
            # Do content or header checks
            # print 'Returning from content rules',url
            return self.check_content_rules(url, parent_url, content, content_type, headers)

        # Check robots.txt
        if not self.flag_ignorerobots:
            self.robots_p.parse_site(url)
            # NOTE: Don't check meta NOW since content of URL has not been downloaded yet.
            if not self.robots_p.can_fetch(url, content=content, meta=False):
                log.extra('Robots.txt rules disallows URL =>',url)
                return False

        # print 'Returning default allowd =>',url
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

        if self.flag_x_robots:
            index, follow = self.robots_p.x_robots_check(url, headers=headers)
            # Don't bother too much with NO index, but bother with NOFOLLOW
            if not follow:
                return False
            
        # Not doing any other content rules now
        return True

class EIIICrawlerStats(crawlerbase.CrawlerStats):
    """ EIII crawler stats class """

    def __init__(self, config):
        super(EIIICrawlerStats, self).__init__()
        self.config = config

    def reset(self):
        super(EIIICrawlerStats, self).reset()
        # URLs downloaded 
        self.urls_d = set()
        # URLs filtered
        self.urls_f = set()
        # All URLs
        self.urls_a = set()
        # URLs with error
        self.urls_e = set()
        
    def update_total_urls_downloaded(self, event):
        """ Update total number of URLs downloaded """

        super(EIIICrawlerStats, self).update_total_urls_downloaded(event)
        self.urls_d.add(event.params.get('url'))

    def update_total_urls_skipped(self, event):
        """ Update total number of URLs skipped """     

        super(EIIICrawlerStats, self).update_total_urls_skipped(event)
        self.urls_f.add(event.params.get('url'))

    def update_total_urls(self, event):
        """ Update total number of URLs """

        # NOTE: This also includes duplicates, URLs with errors - everything.
        super(EIIICrawlerStats, self).update_total_urls(event)
        self.urls_a.add(event.params.get('url'))

    def update_total_urls_error(self, event):
        """ Update total number of URLs that failed to download with error """

        super(EIIICrawlerStats, self).update_total_urls_error(event)
        # The error URLs entry is a tuple of (url, parent_url)
        self.urls_e.add((event.params.get('url'),
                         event.params.get('parent_url')))
            
    def get_stats_dict(self):
        """ Get stats dictionary. """
        statsdict = self.__dict__.copy()

        # statsdict['urls_af'] = list(self.urls_f - self.urls_d)

        mapping = {'urls_a': 'urls_all',
                   'urls_f': 'urls_filtered',
                   'urls_d': 'urls_downloaded',
                   'urls_e': 'urls_error'}

        # print 'URLS_E=>',statsdict['urls_e']
        # Convert sets to list
        for key in ('urls_f','urls_e','urls_d','urls_a'):
            # Make all URL data safe
            entries = list(statsdict[key])
            if key == 'urls_e':
                entries_safe = [map(lambda x: utils.safedata(x), item) for item in entries]
            else:
                entries_safe = map(utils.safedata, entries)
            
            statsdict[mapping.get(key)] = entries_safe
            # Drop original key
            del statsdict[key]

        
        # delete copy
        del statsdict['config']
        return statsdict

    def get_json(self):
        """ Get stats JSON """

        encoder = utils.MyEncoder()
        # Indent by 4 spaces
        encoder.indent = 4

        sdict = self.get_stats_dict()
        # Pickle it
        cPickle.dump(sdict, open('stats.dump','wb'))
        
        # This is a string - eval it and convert to JSON object
        return encoder.encode(sdict)
    
    def publish_stats(self):
        """ Publish stats """
        
        super(EIIICrawlerStats, self).publish_stats()
        # Filtered - downloaded
        # urls_af = list(self.urls_f - self.urls_d)
        # self.urls_f, self.urls_d, self.urls_a = map(list, (urls_af, self.urls_d, self.urls_a))
        # Write stats.json
        statspath = os.path.expanduser(os.path.join(self.config.statsdir, self.config.task_id + '.json'))

        try:
            # import pdb; pdb.set_trace()
            json_data = self.get_json()
            open(statspath,'w').write(json_data)
            log.info("Stats written to",statspath)
        except Exception, e:
            log.error("Error writing stats JSON", str(e))           
        
        try:
            dbpath = os.path.expanduser(os.path.join(self.config.configdir, 'config.db'))
            conn = sqlite3.connect(dbpath)
            c = conn.cursor()
            
            url_string = ','.join(self.config.urls)
            task_id = self.config.task_id
            
            c.execute("""INSERT INTO crawls (crawl_id, urls, statspath)
            VALUES ('%(task_id)s', '%(url_string)s', '%(statspath)s')""" % locals())
            conn.commit()
        except sqlite3.Error, e:
            log.error("Error writing to crawls db", str(e))
        
class EIIICrawler(object):
    """ EIII Web Crawler """

    def __init__(self, urls, cfgfile='config.json', fromdict={}, args=None):
        # Load config from file.
        cfgfile = self.load_config(fname=cfgfile)
        if cfgfile:
            self.config = crawlerbase.CrawlerConfig.fromfile(cfgfile)
        else:
            # Use default config
            print 'Using default configuration...'
            self.config = crawlerbase.CrawlerConfig()

        # Update fromdict if any
        if fromdict:
            # For URL filter, append!
            urlfilter = self.config.url_filter
            self.config.__dict__.update(fromdict)
            self.config.urlfilter += urlfilter

        # Task id
        task_id = self.config.__dict__.get('task_id',uuid.uuid4().hex)
        # Add another crawl log file to the logger
        task_logfile = 'crawl_' + task_id + '.log'
        log.addLogFile(task_logfile)
        
        # Insert task id
        self.config.task_id = task_id
        log.info("Crawl task ID is: ",task_id)
        
        # Prepare it
        self.prepare_config()
        # Prepare config
        self.urls = urls
        # Add to config
        self.config.urls = urls

        # If a param is passed then override it
        if args.param:
            try:
                param, value = args.param.split('=')
                paramtyp = type(getattr(self.config, param))
                log.info("Overriding value of",param,"to",value,"...")
                setattr(self.config, param, paramtyp(value))
                print param,'=>',getattr(self.config, param)
            except ValueError:
                pass

        # Set any override param if specified
        self.empty_count = 0
        # Download queue
        self.dqueue = Queue.Queue()
        # Keeping track of URLs downloaded (downloaded or errored)
        self.url_bitmap = {}
        # Keeping track of URLs put in download queue
        self.url_keys = {None: 1}
        # Crawl stats
        self.stats = EIIICrawlerStats(self.config)
        self.stats.reset()
        
        # Crawl limits enforcement
        self.limit_checker = crawlerbase.CrawlerLimitRules(self.config)
        # print 'Limit checker =>',self.limit_checker
        # Event registry
        self.eventr = CrawlerEventRegistry.getInstance()
        self.subscribe_events()
        # Workers
        self.workers = []
        # Install signal handlers
        # Signal count
        self.sig_count = 0
        # Indicates RED status - set by an event
        # or exception indicating to stop crawl
        # this cant be overridden
        self.red_flag = False

        # URL graph
        self.url_graph = collections.defaultdict(set)
        try:
            signal.signal(signal.SIGINT, self.sighandler)
            signal.signal(signal.SIGTERM, self.sighandler)
        except ValueError:
            # "signal only works in main thread"
            pass
        # Check IDNA encoding for the URLs and encode if necessary.
        self.check_idna_domains()
        if not self.config.disable_dynamic_scope:
            self.set_dynamic_scope()

    def set_dynamic_scope(self):
        """ Do self-adjusting of scope w.r.t the URL given
        If the URL is like http://foo.com use SITE_SCOPE (default)
        But if it is http://foo.com/bar/soap/ then use FOLDER_SCOPE
        """
        
        for url in self.config.urls:
            urlp = urlparse.urlparse(url)
            # Check urlpath after replacing trailing '/' if any
            path = urlp.path.rstrip('/').strip()

            if len(path)>0:
                # URL with a folder like http://foo.com/bar/soap
                # Use folder-scope for full crawl
                utils.info("URL",url,"has non-zero path length. Setting dynamic folder scope for crawling...")
                self.config.site_scope = CrawlPolicy.folder_scope
                break
        
    def get_url_graph(self):
        """ Return the URL graph showing the tree of
        URLs crawled """

        return self.url_graph
    
    def prepare_config(self):
        """ Prepare steps if any for config object """

        plus_rules, minus_rules = [], []
        # Convert URL filter to separate include and exclude ones
        with utils.ignored(AttributeError,):
            for rule_type, rule in self.config.url_filter:
                if rule_type == '+':
                    plus_rules.append(rule)
                else:
                    minus_rules.append(rule)
        
                
        self.config.url_exclude_rules = minus_rules
        self.config.url_include_rules = plus_rules
            
    def sighandler(self, signum, stack):
        """ Signal handler """

        if signum in (signal.SIGINT, signal.SIGTERM,):
            log.info('You interrupted me ! Stopping my work and cleaning up...')
            for worker in self.workers:
                worker.stop()
                
            self.sig_count += 1

        # Set red flag
        self.red_flag = True

        if self.sig_count>1:
            log.info('Force Quitting...')
            # Not exited in natural course, force exiting.
            sys.exit(1)

    def subscribe_events(self):
        """ Subscribe to events """

        self.eventr.subscribe('download_complete', self.url_download_complete)
        self.eventr.subscribe('download_complete_fake', self.url_download_complete)     
        # self.eventr.subscribe('url_pushed', self.url_pushed_callback)       
        self.eventr.subscribe('download_cache', self.url_download_complete)     
        self.eventr.subscribe('download_error', self.url_download_error)
        self.eventr.subscribe('abort_crawling', self.abort_crawl)

    def check_idna_domains(self):
        """ Check if the URL domains are IDNA neutral, if not
        make them ascii safe by IDNA encoding """

        for i in range(len(self.urls)):
            url = self.urls[i]
            
            # Get server
            urlp = urlparse.urlparse(url)
            server = urlp.netloc
            try:
                server.encode('ascii')
            except UnicodeDecodeError, e:
                print 'IDNA encoding URL',url,'...',             
                # Problem ! do idna
                try:
                    server_idna = server.encode('idna')
                except TypeError:
                    # Original string is not unicode
                    server_idna = server.decode('utf-8').encode('idna')                 
                # Replace
                urlp = urlp._replace(netloc=server_idna)
                url_idna = urlparse.urlunparse(urlp)
                print 'new URL is',url_idna,'...'
                self.urls[i] = url_idna
        
    def get(self):
        """ Return the data for crawling """

        return self.dqueue.get()

    def put(self, content_type, url, parent_url=None, key=None):
        """ Push further data to be crawled """

        # optional key is used to figure out if the data
        # has already been pushed - Implementation upto
        # this class.
        if key not in self.url_keys:
            data = (content_type, url, parent_url)
            self.dqueue.put(data)
            self.url_keys[key] = 1

            self.eventr.publish(self, 'url_pushed',
                                message='URL has been pushed to the queue',
                                params=locals())
            # Raise event
            return True

        return False

    def abort_crawl(self, *args):
        """ Stop/abort the crawl """

        # Signal workers to stop
        log.info('Aborting the crawl.')
        for worker in self.workers:
            worker.stop()

        # Set red flag
        self.red_flag = True
        
    def is_empty(self):
        """ Is the work queue empty ? """

        dsz = self.dqueue.qsize()
        log.debug('\tQUEUE SIZE =>', dsz)
        return self.dqueue.empty()

    def workers_idle(self):
        """ Are all workers idle waiting for data ? """

        worker_states = [w.get_state() for w in self.workers]
        log.debug('Worker states =>',worker_states)
        return all((x==0) for x in worker_states)
        # return False
        
    def work_pending(self):
        """ Any work pending ? """

        return (not self.red_flag) and not (self.workers_idle() and self.is_empty())
    
    def check_already_downloaded(self, url):
        """ Is a URL already downloaded """

        return self.url_bitmap.has_key(url)
    
    def url_download_complete(self, event):
        """ Event callback for notifying download for a URL is done """

        # Mark in bitmap
        url = event.params.get('url')
        # Also make entry for original URL
        orig_url = event.params.get('orig_url')
        parent_url = event.params.get('parent_url')
        content_type = event.params.get('content_type','text/html')
        
        # log.debug('Making entry for URL',url,'in bitmap...')
        self.url_bitmap[url] = 1
        if (orig_url != None) and (url != orig_url):
            # log.debug('Making entry for URL',orig_url,'in bitmap...')           
            self.url_bitmap[orig_url] = 1

        if parent_url:
            log.debug("Adding URL ==>",url,"<== to graph for parent ==>",parent_url,"<==")
            self.url_graph[parent_url].add((url, content_type))
                        
    def url_download_error(self, event):
        """ Event callback for notifying download for a URL in error """

        # Mark in bitmap
        url = event.params.get('url')
        orig_url = event.params.get('orig_url')
        
        # log.debug('Making entry for URL',url,'in bitmap...')
        self.url_bitmap[url] = 1

        if url != orig_url:
            # log.debug('Making entry for URL',orig_url,'in bitmap...')           
            self.url_bitmap[orig_url] = 1       

    def make_worker(self):
        """ Make a worker instance """

        return EIIICrawlerQueuedWorker(self.config, self)
    
    def crawl(self):
        """ Do the actual crawling """

        # Demarcating text
        log.logsimple('\n')
        log.logsimple('>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<')
        log.logsimple('>>>>>>>> STARTING CRAWL <<<<<<<<')
        log.logsimple('>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<')
        
        # Push the URLs to queue
        for url in self.urls:
            self.dqueue.put(('text/html',url,None))

        # Mark start time
        self.eventr.publish(self, 'crawl_started')

        nworkers = self.config.num_workers
        
        for i in range(nworkers):
            worker = self.make_worker()
            worker.setDaemon(True)
            
            self.workers.append(worker)
            worker.start()
            # Give subsequent workers some time to start so that the other
            # workers fill in some data.
            time.sleep(10*(nworkers - i))           
        
    def wait(self):
        """ Wait for crawl to finish """
        
        # Wait for some time
        time.sleep(10)

        while self.work_pending():
            time.sleep(5)

        # Push empty values
        [w.stop() for w in self.workers]
        
        self.eventr.publish(self, 'crawl_ended')        
        log.info('Crawl done.')

        print self.url_graph
        self.stats.publish_stats()

    def quit(self):
        """ Clean-up and exit """
        pass

    def _create_config_db(self, dbpath):
        """ Create configuration database when run for the first time """

        conn = sqlite3.connect(dbpath)
        c = conn.cursor()
        # Create tables
        c.execute("""CREATE TABLE crawls
                   (crawl_id varchar(34) NOT NULL primary key,
                    urls varchar(4096) NOT NULL,
                    statspath varchar(128),
                    timestamp datetime default CURRENT_TIMESTAMP) """)
        conn.commit()
        
        
    def load_config(self, fname='config.json'):
        """ Load crawler configuration """

        # Use a default config object for some variables
        cfg = crawlerbase.CrawlerConfig()
        # Look in $HOME/.eiii/crawler folder, then
        # in current folder for a file named config.json
        cfgdir = os.path.expanduser(cfg.configdir)
        dbpath = os.path.join(cfgdir, 'config.db')
        storedir = os.path.expanduser(cfg.storedir)
        statsdir = os.path.expanduser(cfg.statsdir)             
        cfgfile = os.path.join(cfgdir, fname)
        
        if not os.path.isdir(cfgdir):
            print 'First time configuration ...'
            with utils.ignore():
                print 'Config directory',cfgdir,'does not exist. creating ...'
                os.makedirs(cfgdir)
                # Also make store dir
                os.makedirs(storedir)
                os.makedirs(statsdir)
                print 'Making cache structure in store at',storedir,'...'
                utils.create_cache_structure(storedir)
                print 'Saving default configuration to',cfgfile,'...'
                crawlerbase.CrawlerConfig().save(cfgfile)
                # Create database
                self._create_config_db(dbpath)

        # Look in current folder - this overrides the default config file
        if os.path.isfile(fname):
            print 'Using config file...'
            return fname
            
        # Try to load config from default location
        if os.path.isfile(cfgfile):
            print 'Config file found at',cfgfile,'...'
            return cfgfile

    @classmethod
    def parse_options(cls):
        """ Parse command line options """

        if len(sys.argv)<2:
            sys.argv.append('-h')

        parser = argparse.ArgumentParser(prog='eiii_webcrawler',description='Web-crawler for the EIII project - http://eiii.eu')
        parser.add_argument('-v','--version',help='Print version and exit',action='store_true')
        parser.add_argument('-l','--loglevel',help='Set the log level',default='info',metavar='LOGLEVEL')
        parser.add_argument('-c','--config',help='Use the given configuration file',metavar='CONFIG',
                            default='config.json')
        parser.add_argument('-p','--param',help='Override the value of a config param on the command-line',
                            metavar='PARAM')
        parser.add_argument('urls', nargs='*', help='URLs to crawl')

        args = parser.parse_args()
        
        if args.version:
            print 'EIII web-crawler: Version',__version__
            sys.exit(0)

        if len(args.urls)==0:
            print 'No URLs given, nothing to do'
            sys.exit(0)
            
        # Set log level
        if args.loglevel.lower() in ('info','warn','error','debug','critical','extra'):
            log.setLevel(args.loglevel)
        else:
            print 'Invalid log level',args.loglevel
            sys.exit(1)

        return args

    @classmethod
    def main(cls):
        """ Main routine """

        args = cls.parse_options()
        crawler = cls(args.urls, args.config, args=args)
        crawler.crawl()
        crawler.wait()
        crawler.quit()

if __name__ == "__main__":
    # Run this as $ python eiii_crawler.py <url>
    # E.g: python eiii_crawler.py -l debug -c myconfig.json http://www.tingtun.no
    EIIICrawler.main()
