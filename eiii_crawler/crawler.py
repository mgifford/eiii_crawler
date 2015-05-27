# -- coding: utf-8

""" Implementation of EIII crawler """

import sys, os
import Queue
import robocop
import urlparse
import signal
import re
import time
import datetime
import argparse
import collections
import sgmllib
import uuid
import sqlite3
import cPickle
import socket
import multiprocessing
import signal
import gc
import warnings
import copy
import mimetypes
import imp

from eiii_crawler import crawlerbase
from eiii_crawler.crawlerevent import CrawlerEventRegistry, subscribe
from eiii_crawler.crawlerscoping import CrawlPolicy, CrawlerLimitRules, CrawlerScopingRules
from eiii_crawler.crawlerstats import CrawlerStats
from eiii_crawler import urlhelper
from eiii_crawler import utils

# Threaded implementation
from eiii_crawler import threaded
# URL data implementation
from eiii_crawler import urldata

import js.jsparser as jsparser
# top-level plugin module
import eiii_crawler.plugins as eiii_plugins

# Default logging object
log = utils.get_default_logger()

# warnings.simplefilter('ignore')

# Default timeout set to 15s
socket.setdefaulttimeout(15)

__version__ = '1.0a'
__author__ = 'Anand B Pillai'
__maintainer__ = 'Anand B Pillai'

    
class EIIICrawlerQueuedWorker(threaded.ThreadedWorkerBase):
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
        # log.debug("\tGot data =>", data)
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

    def get_url_data_instance(self, url, parent_url=None, content_type='text/html'):
        """ Make an instance of the URL data class
        which fetches the URL """

        return urldata.CachingUrlData(url, parent_url, content_type, self.config)

    def build_url(self, child_url, parent_url):
        """ Build the complete URL for child URL using the parent URL """

        # import pdb; pdb.set_trace()
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

    def _parse(self, data, url):
        """ Parse the HTML and return child URLs """

        # First try with SGML lib based parser
        parser = urlhelper.URLLister()

        try:
            log.info("Parsing URL", url)
            parser.feed(data)
            parser.close()

            self.eventr.publish(self, 'url_parsed',
                                params=locals())
            
        except sgmllib.SGMLParseError, e:
            log.error("Error parsing data for URL =>",url)
            # Try parsing with beautiful soup

            log.info("Parsing with fall-back parser ...")
            parser = urlhelper.SgmlOpParser()
            parser.feed(data)
            # print 'URLS =>', parser.urls
            self.eventr.publish(self, 'url_parsed',
                                params=locals())
            

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
                log.debug("JS Parser Error => ", e)
            

        # If JS redirect don't bother to parse with
        # HTML parser
        if redirect:
            return (url, urls)
        
        return self._parse(data, url)

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
        # log.debug('Checking work pending...',)
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
            # This is a StatusMessage object
            self.eventr.publish(self, 'url_filtered',
                                message=result,
                                message_type='object',
                                params=locals())

        return result
        
    def _allowed(self, url, parent_url=None, content=None, content_type='text/html', headers={},
                 parse=False, download=False):
        """ Is fetching of URL allowed ? - Actual Implementation """                

        # import pdb; pdb.set_trace()
        # Is already downloaded ? Then skip right away
        # NOTE - Do this only for child URLs!
        if (parent_url != None) and (not parse) and self.manager.check_already_downloaded(url):
            return utils.StatusMessage(False, url + ' already downloaded', type='duplicate')

        if (parse) and content_type not in ('text/html','text/xhtml','application/xml','application/xhtml+xml'):
            return utils.StatusMessage(False, "Skipping URL for parsing as mime-type is not (X)HTML or XML", type='mime-type')
        
        if content_type not in self.config.client_mimetypes + self.config.client_extended_mimetypes:
            return utils.StatusMessage(False, 'Skipping URL ' + url + ' as content-type ' + content_type + ' is not valid.',
                                       type='content-type')

        # Part of client mime-types, check if part of fake mime-types
        elif content_type in self.config.client_cheat_mimetypes and download:
           # Simulate download event for this URL so it gets added to URL graph
           # Publish cheat download complete event          
           self.eventr.publish(self, 'download_complete_cheat',
                               message='URL cheated as downloaded',
                               code=200,
                               params=locals())            
           
           return utils.StatusMessage(False,
                                      'Skipping URL ' + url + ' as ' + content_type + 
                                      'is part of cheat mime-types (no download)',
                                      type='content-type',
                                      subtype='fake-mime-type')
        
        
        # If URL include rules are given - the scenario is most likely
        # if these are filtered by some of the other rules - so we should
        # apply them first.
        if any([re.match(rule, url, re.IGNORECASE) for rule in self.config._url_include_rules]):
            return utils.StatusMessage(True, 'Allowing URL ' + url + ' due to specific inclusion rule.',
                                       type='inclusion-rule')

        # Apply exclude rules next
        # print 'Exclusion rules=>',self.config._url_exclude_rules
        if any([re.match(rule, url, re.IGNORECASE) for rule in self.config._url_exclude_rules]):
            # print 'Disallowing URL',url,'due to specific exclusion rule'
            return utils.StatusMessage(False, 'Disallowing URL ' + url + ' due to specific exclusion rule.',
                                       type='exclusion-rule')

        # Dynamic exclusion rules
        if any([re.match(rule, url, re.IGNORECASE) for rule in self.config._url_dynamic_exclude_rules]):
            # Uncomment following for printing message during dynamic exclusion
            # log.extra('Disallowing URL ',url,' due to dynamic exclusion rule.')
            # Add this to a list
            return utils.StatusMessage(False, 'Disallowing URL ' + url + ' due to dynamic exclusion rule.',
                                       type='dynamic-exclusion-rule', subtype=rule)

        # Scoping rules
        if parent_url != None:
            scoper = CrawlerScopingRules(self.config, parent_url)
            
            # Proceed further - do site scoping rules
            m_allowed = scoper.allowed(url, parent_url, content_type)
            if not m_allowed:
                # Get the message
                error_msg = str(m_allowed)
                return utils.StatusMessage(False, 'Scoping rules does not allow URL [Error: %s]'  % error_msg,
                                           type=m_allowed.type, subtype=m_allowed.subtype)
        else:
            log.extra('Parent URL is none =>', url)
                        
        if (content != None) or len(headers):
            # Do content or header checks
            # print 'Returning from content rules',url
            return self.check_content_rules(url, parent_url, content, content_type, headers)

        # Check robots.txt
        if not self.flag_ignorerobots:
            status, msg = self.robots_p.parse_site(url)
            if not status:
                log.error("Error fetching/parsing robots.txt rules for",url,": robots.txt would be ignored")
                log.error("\t=>",msg)
                # Don't bother to check as now robots.txt rules don't apply
                return utils.StatusMessage(True, 'Error fetching/parsing robots.txt rules for "%s" robots.txt would be ignored' % url,
                                           type='robots', subtype='robots.txt')
            
            # NOTE: Don't check meta NOW since content of URL has not been downloaded yet.
            if not self.robots_p.can_fetch(url, content=content, meta=False):
                log.extra('Robots.txt rules disallows URL =>',url)
                return utils.StatusMessage(False, 'Robots.txt rules disallows URL %s' % url,
                                           type='robots', subtype='robots.txt')                

        return utils.StatusMessage(True, 'Default allowed')


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
                log.extra('META robots rules disallows URL =>',url)
                return utils.StatusMessage(False, 'META robots rules disallows URL "%s"' % url,
                                           type='robots', subtype='meta-robots')              

        if self.flag_x_robots:
            index, follow = self.robots_p.x_robots_check(url, headers=headers)
            # Don't bother too much with NO index, but bother with NOFOLLOW
            if not follow:
                log.extra('x-robots rules disallows URL =>',url)
                return utils.StatusMessage(False, 'X-robots rules disallows URL "%s"' % url,
                                           type='robots', subtype='x-robots')                            
            
        # Not doing any other content rules now
        return utils.StatusMessage(True, 'Default allowed') 

class EIIICrawlerStats(CrawlerStats):
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
        # Dynamic URLs filtered
        self.urls_fd = collections.defaultdict(list)
        # All URLs
        self.urls_a = set()
        # URLs with error
        self.urls_e = set()
        # URL graph
        self.url_graph = collections.defaultdict(set)
        
    def update_total_urls_downloaded(self, event):
        """ Update total number of URLs downloaded """

        super(EIIICrawlerStats, self).update_total_urls_downloaded(event)
        self.urls_d.add(event.params.get('url'))

    def update_total_urls_skipped(self, event):
        """ Update total number of URLs skipped """     

        error_msg = event.message
        super(EIIICrawlerStats, self).update_total_urls_skipped(event)
        url = event.params.get('url')
        self.urls_f.add(url)
        
        # Add to dynamic URLs filtered dictionary if dynamic filtering
        if error_msg.type == 'dynamic-exclusion-rule':
            self.urls_fd[error_msg.subtype].append(url)
        
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

    def update_url_download(self, parent_url, url, content_type):
        """ Update URL download information """

        if parent_url:
            log.debug("Adding URL ==>",url,"<== to graph for parent ==>",parent_url,"<==")
            self.url_graph[parent_url].add((url, content_type))
        else:
            # Child itself is the parent - i.e top level URL, add empty children
            self.url_graph[url] = set()         

    def normalize(self, entries):
        """ Normalize the URLs in the URL graph. This drops duplicate URLs
        based on trailing / and also fixes content-type errors """

        urlbucket = {}
        
        entries_fixed = []

        for url,ctype in entries:
            # Fix for issue #434
            # Drop trailing / from the URL if any
            if url[-1] == '/':
                url_entry = url[:-1]
            else:
                url_entry = url[:]

            # If URLs are not case sensitive, convert to lower-case
            # NOTE: Be careful disabling this as it might drop URLs which
            # are not duplicates. 
            if not self.config.flag_urls_case_sensitive:
                url_entry = url_entry.lower()
                
            # Skip URLs already in the bucket
            if url_entry in urlbucket: continue

            # Make an entry
            urlbucket[url_entry] = 1
            
            if ctype.startswith('text/'):
                # Guess again
                ctype_new = urlhelper.guess_content_type(url)
                if ctype != ctype_new and not ctype_new.startswith('text/'):
                    # Skip it
                    log.debug('Content-type of',url,'fixed from',ctype,'to',ctype_new,'skipping.')
                    continue

            # Only append valid client mime-types
            if ctype in self.config.client_mimetypes:
                entries_fixed.append((url, ctype))                  

        return entries_fixed
    
    def get_stats_dict(self):
        """ Get stats dictionary. """

        statsdict = self.__dict__.copy()

        mapping = {'urls_a': 'urls_all',
                   'urls_f': 'urls_filtered',
                   'urls_fd': 'urls_dynamic_filtered',
                   'urls_d': 'urls_downloaded',
                   'urls_e': 'urls_error'}

        # print 'URLS_E=>',statsdict['urls_e']
        # Convert sets to list
        for key in ('urls_f', 'urls_e','urls_d','urls_a'):
            # Make all URL data safe
            entries = list(statsdict[key])
            if key == 'urls_e':
                entries_safe = [map(lambda x: utils.safedata(x), item) for item in entries]
            else:
                entries_safe = map(utils.safedata, entries)
            
            statsdict[mapping.get(key)] = entries_safe
            # Drop original key
            del statsdict[key]

        statsdict['urls_audio_visual'] = []
        
        for url in statsdict['urls_all']:
            ctype = mimetypes.guess_type(url)
            if (len(ctype) == 0) or (ctype[0] == None): continue
            if any(map(lambda x: ctype[0].startswith(x), ('audio/','video/', 'application/x-shockwave-flash'))):
                # This is an A/V URL
                statsdict['urls_audio_visual'].append((url, ctype[0]))              

        # Dictionary items
        for key in ('urls_fd',):
            entries = statsdict[key]
            entries_safe = {}
            
            for regex, urls in entries.items():
                urls_safe = map(utils.safedata, urls)
                entries_safe[regex] = urls_safe

            statsdict[mapping.get(key)] = entries_safe
            
        # Process URL graph - make a copy as we will be modifying it.
        graph = copy.deepcopy(self.url_graph)
        urlbucket = {}
        
        for parent_url in graph.keys():
            # Fix for issue #434
            # Drop trailing / from the URL if any
            if parent_url[-1] == '/':
                url_entry = parent_url[:-1]
            else:
                url_entry = parent_url[:]

            # Skip URLs already in the bucket
            if url_entry in urlbucket: continue

            # Make an entry
            urlbucket[url_entry] = 1
            entries = graph[parent_url]
            
            # Child URLs
            entries = list(entries)
            # Make URLs safe
            entries_safe =  [(utils.safedata(url), ctype) for url, ctype in entries]
            # Fix wrong content-type for "text" types.
            entries_safe = self.normalize(entries_safe)
            
            # Make entry safe
            parent_url_safe = utils.safedata(parent_url)
            if parent_url_safe != parent_url:
                # Delete original key
                del graph[parent_url]

            # Add entry
            graph[parent_url_safe] = entries_safe

        statsdict['url_graph'] = graph
        
        # delete copy
        del statsdict['config']

        for key,val in statsdict.items():
            if type(val) is datetime.datetime:
                statsdict[key] = str(val)
            
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
        # Write stats.json
        statspath = os.path.expanduser(os.path.join(self.config.statsdir, self.config._task_id + '.json'))

        try:
            # import pdb; pdb.set_trace()
            json_data = self.get_json()
            open(statspath,'w').write(json_data)
            log.info("Stats written to",statspath)
        except Exception, e:
            raise
            log.error("Error writing stats JSON", str(e))           
        
        try:
            dbpath = os.path.expanduser(os.path.join(self.config.configdir, 'config.db'))
            conn = sqlite3.connect(dbpath)
            c = conn.cursor()
            
            url_string = ','.join(self.config._urls)
            task_id = self.config._task_id
            
            c.execute("""INSERT INTO crawls (crawl_id, urls, statspath)
            VALUES ('%(task_id)s', '%(url_string)s', '%(statspath)s')""" % locals())
            conn.commit()
        except sqlite3.Error, e:
            log.error("Error writing to crawls db", str(e))
        except UnicodeDecodeError, e:
            pass
        
class EIIICrawler(multiprocessing.Process):
    """ EIII Web Crawler """

    def __init__(self, urls=[], cfgfile='config.json', fromdict={},
                 args=None, task_queue=None, value_dict=None, state=None):

        # Load config from file.
        cfgfile = self.load_config(fname=cfgfile)
        if cfgfile:
            try:
                self.config = crawlerbase.CrawlerConfig.fromfile(cfgfile)
            except Exception, e:
                print 'Error:',str(e)
                sys.exit(1)
        else:
            # Use default config
            print 'Using default configuration...'
            self.config = crawlerbase.CrawlerConfig()

        # Default URL filter
        self.default_url_filter = self.config.url_filter[:]
        
        # Update fromdict if any
        if fromdict:
            self.config.url_filter = []
            # For URL filter, append!
            self.config.update(fromdict)
            self.config.url_filter += self.default_url_filter
            # log.extra('URL FILTER=>',self.config.url_filter)

        # Task queue - Used when crawler is run as an independent
        # process through the crawler server
        self.taskq = task_queue
        # Value dictionary - return value of crawl is copied
        # here when the crawler is called from the server.
        self.value_dict = value_dict
        # Shared state between server and the crawler
        self.state = state
        
        # Crawler ID
        self.id = 'Crawler-' + str(uuid.uuid1())
        
        # Task id
        task_id = self.config.__dict__.get('task_id', str(uuid.uuid4()))
        # Insert task id
        self.config._task_id = task_id

        if len(urls):
            self.task_logfile = utils.get_logfilename(task_id, urls, self.config)
            log.addLogFile(self.task_logfile)
            log.info("Log file for this crawl can be found at", os.path.abspath(self.task_logfile))
        
        # Prepare it
        self.prepare_config()
        # Prepare config
        self.urls = urls
        # Add to config
        self.config._urls = urls

        # If a param is passed then override it
        if args and args.param:
            try:
                param, value = args.param.split('=')
                paramtyp = type(getattr(self.config, param))
                log.info("Overriding value of",param,"to",value,"...")
                setattr(self.config, param, paramtyp(value))
                print param,'=>',getattr(self.config, param)
            except ValueError:
                pass

        # Crawl stats
        self.stats = EIIICrawlerStats(self.config)
        # Crawl limits enforcement
        self.limit_checker = CrawlerLimitRules(self.config)
        
        # print 'Limit checker =>',self.limit_checker
        # Event registry
        self.eventr = CrawlerEventRegistry.getInstance()
        self.subscribe_events()

        self.reset()
        multiprocessing.Process.__init__(self, None, None, name='Crawler ' + self.id)        

    def reset(self):
        """ Reset previous crawling state if any """

        log.info('===>RESETTING CRAWLER STATE<===')
        # Set any override param if specified
        self.empty_count = 0
        # Download queue
        self.dqueue = Queue.Queue()
        # Keeping track of URLs downloaded (downloaded or errored)
        self.url_bitmap = {}
        # Keeping track of URLs put in download queue
        self.url_keys = {None: 1}
        # Workers
        self.workers = []
        # Install signal handlers
        # Signal count
        self.sig_count = 0
        # Indicates RED status - set by an event
        # or exception indicating to stop crawl
        # this cant be overridden
        self.red_flag = False
        # Indicates crawler is busy
        self.busy = False
        # Server flag - used by the Crawler server only
        self.server_flag = True
        # Crawl failure message - when the starting URL
        # doesn't take off
        self.fatal_msg = { 'msg': '',
                           'type': '',
                           'subtype': '',
                           'url': '' }
        
        self.stats.reset()
        self.limit_checker.reset()
        self.eventr.reset()
        
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

        # Load plugins
        self.load_plugins()

    def load_plugins(self):
        """ Load plugin modules which are enabled in the configuration """

        for plugin in self.config.plugins:
            print 'Loading plugin',plugin,'...'
            # mod = getattr(eiii_plugins, plugin)
            m = imp.find_module(plugin, eiii_plugins.__path__)
            mod = imp.load_module(plugin, *m)
            # If module defines a setup function, call it
            try:
                setup = getattr(mod, 'set_config')
                # get configuration
                plugin_conf = self.config.plugin_conf.get(plugin)
                setup(**plugin_conf)
            except AttributeError:
                print 'Plugin',mod,'defines no set_config function.'

    def set_dynamic_scope(self):
        """ Do self-adjusting of scope w.r.t the URL given
        If the URL is like http://foo.com use SITE_SCOPE (default)
        But if it is http://foo.com/bar/soap/ then use FOLDER_SCOPE
        """
        
        for url in self.config._urls:
            urlp = urlparse.urlparse(url)
            # Check urlpath after replacing trailing '/' if any
            path = urlp.path.rstrip('/').strip()

            if len(path)>0:
                # URL with a folder like http://foo.com/bar/soap
                # Use folder-scope for full crawl
                log.info("URL",url,"has non-zero path length. Setting dynamic folder scope for crawling...")
                self.config.site_scope = CrawlPolicy.folder_scope
                break
        
    def get_url_graph(self):
        """ Return the URL graph showing the tree of
        URLs crawled """

        return self.stats.url_graph
    
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
        
        self.config._url_exclude_rules = minus_rules
        self.config._url_include_rules = plus_rules

        # Initialize dynamic exclusion rules 
        self.config._url_dynamic_exclude_rules = []
        self.config._dynamic_rules_stats = collections.defaultdict(int)
            
    def sighandler(self, signum, stack):
        """ Signal handler """

        if signum in (signal.SIGINT, signal.SIGTERM,):
           log.info('You interrupted me ! Stopping my work and cleaning up...')
           # Close the queue and join background thread
           if self.taskq != None:
               self.taskq.close()
               self.taskq.join_thread()
           
           for worker in self.workers:
               worker.stop()
               
           self.sig_count += 1

        # Set red flag
        self.red_flag = True
        sys.exit(1)
        
        if self.sig_count>1:
           log.info('Force Quitting...')
           # Not exited in natural course, force exiting.
           sys.exit(1)

    def subscribe_events(self):
        """ Subscribe to events """

        self.eventr.subscribe('download_complete', self.url_download_complete)
        self.eventr.subscribe('download_complete_fake', self.url_download_complete)
        self.eventr.subscribe('download_complete_cheat', self.url_download_complete)            
        self.eventr.subscribe('download_cache', self.url_download_complete)     
        self.eventr.subscribe('download_error', self.url_download_error)
        self.eventr.subscribe('abort_crawling', self.abort_crawl)
        self.eventr.subscribe('worker_threw_exception', self.replace_worker)
        self.eventr.subscribe('url_filtered', self.url_filtered)
        self.eventr.subscribe('url_not_allowed', self.url_filtered)     

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
        # log.debug('\tQUEUE SIZE =>', dsz)
        return self.dqueue.empty()

    def workers_idle(self):
        """ Are all workers idle waiting for data ? """

        worker_states = [w.get_state() for w in self.workers]
        # log.debug('Worker states =>',worker_states)
        return all((x==0) for x in worker_states)
        # return False
        
    def work_pending(self):
        """ Any work pending ? """

        # print '====> RED FLAG:',self.red_flag
        # print '====> WORKERS IDLE:',self.workers_idle()
        # print '====> EMPTY:',self.is_empty()
        
        return (not self.red_flag) and not (self.workers_idle() and self.is_empty())
    
    def check_already_downloaded(self, url):
        """ Is a URL already downloaded """

        # Fix #434 here as well
        # Drop trailing / from the URL if any
        # if url[-1] == '/': url = url[:-1]
        urlp = urlparse.urlparse(url)
        url_no_scheme = url.replace(urlp.scheme + '://', '')
        return self.url_bitmap.has_key(url_no_scheme)

    def url_filtered(self, event):
        """ Event callback for notifying when a URL is filtered """

        url = event.params.get('url')
        parent_url = event.params.get('parent_url')

        # If this is the start URL (parent_url==None) then keep
        # the error message.

        # This is error message object
        error_msg = event.message
        
        # Log it at debug level
        if len(str(error_msg)):
            log.debug(error_msg)

            # If parent URL is None, this often means the crawl doesn't
            # start as the starting URL itself is filtered, so that
            # message is important, hence log it.

            # Also log it if parent_url is the start URL since it means
            # getting filtered at next level.
            if (parent_url == None) or (parent_url in self.urls):
                self.fatal_msg['msg'] = str(error_msg)
                self.fatal_msg['type'] = error_msg.type
                self.fatal_msg['subtype'] = error_msg.subtype
                self.fatal_msg['url'] = url
                self.fatal_msg['parent_url'] = '' if (parent_url==None) else parent_url
                
                # log.debug("Logging crawl fatal error", error_msg)

    def url_download_complete(self, event):
        """ Event callback for notifying download for a URL is done """

        # Mark in bitmap
        url = event.params.get('url')
        # Also make entry for original URL
        orig_url = event.params.get('orig_url')
        parent_url = event.params.get('parent_url')
        content_type = event.params.get('content_type','text/html')

        # Issue #438 - drop scheme when storing in bitmap
        urlp = urlparse.urlparse(url)
        url_no_scheme = url.replace(urlp.scheme + '://', '')
        
        # log.debug('Making entry for URL',url,'in bitmap...')
        self.url_bitmap[url_no_scheme] = 1

        if (orig_url != None) and (url != orig_url):
            urlp = urlparse.urlparse(orig_url)          
            orig_url_no_scheme = orig_url.replace(urlp.scheme + '://', '')            
            # log.debug('Making entry for URL',orig_url,'in bitmap...')           
            self.url_bitmap[orig_url_no_scheme] = 1

        self.stats.update_url_download(parent_url, url, content_type)
                        
    def url_download_error(self, event):
        """ Event callback for notifying download for a URL in error """

        # Mark in bitmap
        url = event.params.get('url')
        orig_url = event.params.get('orig_url')
        parent_url = event.params.get('parent_url')
        error_msg = event.message
        
        # log.debug('Making entry for URL',url,'in bitmap...')
        self.url_bitmap[url] = 1

        if url != orig_url:
            # log.debug('Making entry for URL',orig_url,'in bitmap...')           
            self.url_bitmap[orig_url] = 1

        if len(error_msg):
            log.debug(error_msg)

            # If parent URL is None, this often means the crawl doesn't
            # start as the starting URL itself is filtered, so that
            # message is important, hence log it.
            if parent_url == None:
                self.fatal_msg['msg'] = error_msg
                self.fatal_msg['type'] = 'download'
                self.fatal_msg['url'] = url
                self.fatal_msg['parent_url'] = ''
                
                # We can do some heuristics here.
                if error_msg.lower().find('name or service not known') != -1:
                    self.fatal_msg['subtype'] = 'dns'

                # Can add more sub-types later.
                # log.debug("Logging crawl fatal error", error_msg)          

    def make_worker(self):
        """ Make a worker instance """

        return EIIICrawlerQueuedWorker(self.config, self)

    def replace_worker(self, event):
        """ Replace a killed worker with a new one """

        # the killed thread.
        thread = event.publisher
        log.info("Thread",thread,"died.")
        
        # Find current index of this thread
        for i in range(len(self.workers)):
            t = self.workers[i]
            if t == thread:
                # Replace at this position
                log.info("Making new worker at index",i,"replacing",thread,"...")
                worker = self.make_worker()
                self.workers[i] = worker
                worker.start()
                break
                       
    def run(self):
        """ Process entry method when the crawler is used
        as a multiprocessing crawler from the EIII crawler server """

        # Issue #426 - turn off global crawl.log for server crawls
        log.removeLogFile(utils.get_crawl_log())
        log.debug("Starting Crawler Process =>", self.id)
        
        while self.server_flag:
            log.info(self.id,"=> waiting on task queue from server ...")
            try:
                execute = self.taskq.get()
            except (IOError, ValueError), e:
                # To exit at shut-down (Ctrl-C, termination) times
                print e
                break
            
            log.info(self.id,"=> obtained task queue from server ...")         
            # Execute consists of urls and configdict
            urls, configdict  = execute
            # Set log level
            loglevel = configdict.get('loglevel', 'info')
            log.info('Setting log level to' , loglevel)
            log.setLevel(loglevel)
            
            # Set state to busy
            self.state[self.id] = 1
            self.crawl_using(urls, configdict)
            self.wait_crawl()

            # Set state to idling
            self.state[self.id] = 0
            log.debug("Crawler",self.id,"done crawl",self.server_flag)
            
    def crawl_using(self, urls, fromdict):
        """ Crawl using the given URLs and config dictionary.
        This is the API used by the EIII crawler server. """

        # Use this API if you initialize a single crawler object
        # and want to reuse it for multiple crawls in the same
        # session - the EIII crawler server use-case.
        
        if fromdict:
            # For URL filter, reset and append
            self.config.url_filter = []
            self.config.update(fromdict)
            self.config.url_filter += self.default_url_filter           

        # Task id
        task_id = self.config.__dict__.get('task_id',uuid.uuid4().hex)
        
        # Add another crawl log file to the logger
        self.task_logfile = utils.get_logfilename(task_id, urls, self.config)
        log.addLogFile(self.task_logfile)
        # log.info("Log file for this crawl can be found at", os.path.abspath(self.task_logfile))     
        
        # Insert task id
        self.config._task_id = task_id
        
        # Prepare it
        self.prepare_config()
        # Prepare config
        self.urls = urls
        # Add to config
        self.config._urls = urls
        self.config.save('crawl.json')

        log.setConsole(False)
        self.reset()        
        self.crawl()
        # log.setConsole(True)        
                    
    def crawl(self):
        """ Do the actual crawling """

        log.info("Crawler",self.id,"starting crawl of task id",self.config._task_id,"...")
        
        self.busy = True
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

    def wait_crawl(self):
        """ Waiting method used when crawler is run as a separate
        process through the EIII crawler server """

        # Wait for some time
        time.sleep(10)

        while self.work_pending():
            time.sleep(5)
                
        self.eventr.publish(self, 'crawl_ended')        
        log.info('Crawl done.')

        # print self.url_graph
        self.stats.publish_stats()
        log.info("Log file for this crawl can be found at", os.path.abspath(self.task_logfile))
        # log.info(utils.bye_message())
        
        # Get the graph
        url_graph = self.get_url_graph()
        stats_dict = self.stats.get_stats_dict()

        self.value_dict[self.config._task_id] = {'stats': stats_dict,
                                                 'graph': url_graph,
                                                 'error': self.fatal_msg}
        # Force gc collection
        gc.set_debug(gc.DEBUG_STATS|gc.DEBUG_COLLECTABLE|gc.DEBUG_UNCOLLECTABLE)
        gc.collect()

    def stop_server(self):
        """ Stop method used only by the Crawler Server to stop the crawl process """

        # Set red flag
        self.red_flag = True
        # Set server flag off
        self.server_flag = False
        
        # Only used in abnormal conditions or to interrupt the crawl
        for worker in self.workers:
            worker.stop()

        print 'Crawler process =>',self.id,'stopped.'
        
        # log.info("Crawl Process =>",self.id,"stopped.")
        
    def wait(self):
        """ Wait for crawl to finish """
        
        # Wait for some time
        time.sleep(10)

        while self.work_pending():
            time.sleep(5)

        # Push empty values
        [w.stop() for w in self.workers]
        # [w.join() for w in self.workers]
        
        self.eventr.publish(self, 'crawl_ended')        
        log.info('Crawl done.')

        # Wait a bit
        time.sleep(2)
        
        # print self.url_graph
        self.stats.publish_stats()
        log.info("Log file for this crawl can be found at", os.path.abspath(self.task_logfile))

    def quit(self):
        """ Clean-up and exit """

        log.info("Goodbye.")
        # log.info(utils.bye_message())
        
        sys.exit(0)

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
            print 'Using config file',fname,'...'
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
        parser.add_argument('-p','--param',help='Override the value of a config param on the command-line (e.g: -p "flag_ignorerobots=True")', metavar='PARAM')
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
