
#!/usr/bin/env python

from ttrpc.server import SimpleTTRPCServer, UserError
import time
import datetime
import sys
import os
import gc
import signal
import multiprocessing
import threading

from eiii_crawler.eiii_crawler import utils
from eiii_crawler.eiii_crawler import EIIICrawler, log
from eiii_crawler.crawlerstats import CrawlerStats

class EIIICrawlerServer(SimpleTTRPCServer):
    """ EIII crawler server obeying the tt-rpc protocol """

    class _LoggerWrapper(object):
        def __getattr__(self, name):
            f = getattr(log, name)
            def wrapper(s, *args):
                return f(s % args)
            return wrapper

    _logger = _LoggerWrapper()

    def __init__(self, nprocs=10):
        # All the crawler objects
        self.instances = []
        # Tasks queue
        self.task_queue = multiprocessing.Queue()
        # Dictionary used to share return-values from
        # crawler processing
        self.manager = multiprocessing.Manager()
        # Return shared state dictionary shared with crawler processes
        self.return_dict = self.manager.dict()
        # Maxium number of crawl instances
        self.nprocs = nprocs
        
        SimpleTTRPCServer.__init__(self)
        try:
            signal.signal(signal.SIGINT, self.sighandler)
            signal.signal(signal.SIGTERM, self.sighandler)
        except ValueError:
            # "signal only works in main thread"
            pass
        
        self.init_crawler_procs()
        
    def _packer_default(self, obj):
        strtypes = (datetime.datetime, datetime.date, datetime.timedelta)
        if any(isinstance(obj, x) for x in strtypes):
            return str(obj)
        return obj

    def sighandler(self, signum, stack):
        """ Signal handler """

        if signum in (signal.SIGINT, signal.SIGTERM,):
            for crawler in self.instances:
                log.info("Stopping Crawler",crawler.id)
                crawler.stop_server()
                time.sleep(1)
                
            sys.exit(1)
            
    def ping(self, ctl):
        return "pong"

    def init_crawler_procs(self):
        """ Start a pool of crawler processes """
        
        for i in range(self.nprocs):
            # Make a new instance
            crawler = EIIICrawler(task_queue = self.task_queue,
                                  value_dict = self.return_dict)
            log.info("Initialized Crawler ", crawler.id)
            crawler.server_flag = True
            
            self.instances.append(crawler)
            crawler.start()

    def do_crawl(self, ctl, crawler_rules):
        """ Perform crawling """

        if ctl: ctl.setStatus("Starting crawl ...")
        print 'Starting crawl ...'
        # Map crawler rules into internal Rules.
        config_dict = utils.convert_config(crawler_rules)
        urls = config_dict.get('urls')

        if urls==None:
            print 'No URLs given!'
            raise UserError(0, "No URLs given!")

        # Set log level
        log.setLevel(crawler_rules.get('loglevel','info'))

        # Set task id
        config_dict['task_id'] = ctl.id_
        # Push the task
        task = (urls, config_dict)
        self.task_queue.put(task)

        return self.poll(ctl, ctl.id_)
    
    def poll(self, ctl, task_id):
        """ Poll for crawl results - done by the client
        which crawls the server """

        # Poll for result 
        # NOTE: This is better done with a synchronization primitive
        # like a shared semaphore which automatically makes the
        # client wait till the crawl finishes. However the semantics
        # for shared memory semaphores and the synchronization is sometimes
        # tricky to get right - so right now a polling is done as it
        # though stupid, is simple.
        print 'Calling poll for results...'
        while not self.return_dict.has_key(task_id):
            print 'Client waiting...',task_id,'...'
            time.sleep(3)

        return_data = self.return_dict[task_id]
        url_graph = return_data['graph']
        stats_dict = return_data['stats']
        
        return { 'result': self.make_directed_graph(url_graph),
                 'stats': stats_dict,
                 '__type__': "crawler-result"}
        
    def crawl(self, ctl, crawler_rules, threaded=True):
        """ Accepts a crawler control object, dictionary of
        crawler rules and start crawling """

        # if threaded:
        #    t = threading.Thread(None, self.do_crawl, 'Crawl-'+ctl.id_,(ctl, crawler_rules))
        #    t.start()
        return self.do_crawl(ctl, crawler_rules)

    def getresult(self, ctl, taskid):
        """ Return results for a task id """

        return_data = self.return_dict.get(taskid)
        if return_data != None:
            url_graph = return_data['graph']
            stats_dict = return_data['stats']
        
            return { 'result': self.make_directed_graph(url_graph),
                     'stats': stats_dict,
                     '__type__': "crawler-result"}
        else:
            print 'No result found for task',taskid
            return {}
    
    def make_directed_graph(self, url_graph):
        """ Convert the URL graph data structure obtained
        from crawler into a directed graph

        Example:
        
        [ ("text/html", "http://tingtun.no/", [0,1,2])
        , ("text/html", "http://tingtun.no/search", [0,1,2])
        , ("text/html", "http://tingtun.no/research", [0,1,2,3])
        , ("application/pdf", "http://tingtun.no/research/some.pdf", [])
        ]

        """

        url_set = set()
        url_ctype = {}
        
        # Add all to the set
        for url_key, url_values in url_graph.items():
            url_set.add(url_key)
            # Keys are always HTML since they are parents
            url_ctype[url_key] = 'text/html'

            # print url_values
            for url, ctype in url_values:
                # print 'URL =>',url,ctype
                url_set.add(url)
                url_ctype[url] = ctype

        # Make a list out of it
        url_list = sorted(list(url_set))
        # print url_list
        
        url_dgraph = []

        for url in url_list:
            ctype = url_ctype[url]
            child_index = []
            
            # Do I have children ?
            if url in url_graph:
                # Find indices of children, since we are
                # going through the same list, index will
                # be shared across both lists.
                child_urls = map(lambda x: x[0], url_graph[url])
                for child_url in child_urls:
                    child_index.append(url_list.index(child_url))
            else:
                # No child URLs
                pass

            url_dgraph.append((ctype, url, child_index))

        print 'URL directed graph=>',url_dgraph
        return url_dgraph
        
    def load(self, ctl):
        """
        Returns a number. If it's above 100, it's overloaded. The
        chance of getting a crawl call is proportional to how much below
        100 it is compared to the other servers in rotation.

        """

        # Keeping this for the time being.
        return int(float(open("/proc/loadavg").read().split()[0]) * 100)

          
if __name__ == "__main__":
    crawler_rules = {'max-pages': [(['text/html', 'application/xhtml+xml', 'application/xml'], 50)],
                     'scoping-rules': [('+', '^https?://utt\\.tingtun\\.no')], 'min-crawl-delay': 2,
                     'size-limits': [(['text/html', 'application/xhtml+xml', 'application/xml'], 500)],
                     'seeds': ['http://docs.python.org/library/'], 'obey-robotstxt': 'false',
                     'loglevel': 'debug'}

    port=8910
    print 'Starting crawler server on port',port,'...'
    # Number of crawler processes to start
    nprocs = 10
    # An equivalent number (or greater) of processes need to be
    # started on the tt-rpc server for proper 1:1 scaling.
    EIIICrawlerServer(nprocs=nprocs).listen("tcp://*:%d" % port, nprocs=nprocs)

    
