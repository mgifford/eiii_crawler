#!/usr/bin/env python

from ttrpc.server import SimpleTTRPCServer, UserError
from ttrpc.client import TTRPCProxy
import time
import datetime
import sys
import os
import signal
import multiprocessing
import threading
import argparse
import traceback

try:
    from eiii_crawler import utils
except ImportError:
    from eiii_crawler.eiii_crawler import utils
try:
    from eiii_crawler.crawler import EIIICrawler, log
except ImportError:
    from eiii_crawler.crawler import EIIICrawler, log
try:
    pass
except ImportError:
    pass

pidfile = '/tmp/eiii_crawler_server.pid'
def fix_url_graph(url_graph):
    """ Fix or remove invalid URLs in URL graph """

    url_graph_f = {}

    # Issue 470 - stand-alone % characters
    for url in url_graph.keys():
        url = utils.fix_quoted_url(url)
        children = url_graph[url]
        children2 = set()

        for child_url, ctype in children:
            child_url = utils.fix_quoted_url(child_url)
            children2.add((child_url, ctype))

        url_graph_f[url] = children2

    return url_graph_f

def make_directed_graph(url_graph):
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
    # Bug: sorted gives a UnicodeDecodeError if an impoperly encoded unicode
    # URL is present in the list - see http://gitlab.tingtun.no/eiii/eiii_crawler/issues/411
    # Sorted is possibly not needed .

    # url_list = sorted(list(url_set))
    url_list = list(url_set)
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

    # print 'URL directed graph=>',url_dgraph
    return url_dgraph


class EIIICrawlerServer(SimpleTTRPCServer):
    """ EIII crawler server obeying the tt-rpc protocol """

    class _LoggerWrapper(object):
        def __getattr__(self, name):
            f = getattr(log, name)
            def wrapper(s, *args):
                return f(s % args)
            return wrapper

    _logger = _LoggerWrapper()

    def __init__(self, nprocs=10, loglevel='info',bus_uri=None,port=8910,bind_addr='127.0.0.1'):
        open(pidfile, 'w').write(str(os.getpid()))
        # All the crawler objects
        self.instances = []
        # Number of tasks
        self.ntasks = 0
        # Tasks queue
        self.task_queue = multiprocessing.Queue()
        # Dictionary used to share return-values from
        # crawler processing
        self.manager = multiprocessing.Manager()
        # Return shared state dictionary shared with crawler processes
        self.return_dict = self.manager.dict()
        # Shared state - indicates crawler activity
        self.state = self.manager.dict()
        # Maxium number of crawl instances
        self.nprocs = nprocs
        # Log level
        self.loglevel = loglevel
        self.bus_url = None
        self.bus_uri = None
        
        SimpleTTRPCServer.__init__(self)
        
        if bus_uri:
            self.bus_uri = bus_uri
            # Register on bus
            self.bus_url='tcp://' + bind_addr + ':' + str(port)
            proxy = TTRPCProxy(bus_uri,retries=3,timeout=6000)
            if proxy.ping() == 'pong':
                # We need to fork this off to a separate thread so that we are
                # free to handle the call which the bus makes back to us
                t = threading.Thread(target=proxy.Call(proxy,'add-component'),
                                     args=('crawler', self.bus_url))
                t.start()

        self.init_crawler_procs()

        signal.signal(signal.SIGINT, self.sighandler)
        signal.signal(signal.SIGTERM, self.sighandler)
        
    def _packer_default(self, obj):
        strtypes = (datetime.datetime, datetime.date, datetime.timedelta)
        if any(isinstance(obj, x) for x in strtypes):
            return str(obj)
        return obj

    def sighandler(self, signum, stack):
        """ Signal handler """

        if signum in (signal.SIGINT, signal.SIGTERM,):
            # Close the queye
            self.task_queue.close()
            # Unregister from bus if defined
            if self.bus_uri != None:
                proxy = TTRPCProxy(self.bus_uri,retries=3,timeout=6000)
                if proxy.ping() == 'pong':              
                    print 'Unregsitering from bus.'
                    # We need to fork this off to a separate thread so that we are
                    # free to handle the call which the bus makes back to us
                    t = threading.Thread(target=proxy.Call(proxy,'rm-crawler'),
                                         args=(self.bus_url,))
                t.start()
                t.join()
                
            sys.exit(1)
            
    def ping(self, ctl):
        return "pong"

    def init_crawler_procs(self):
        """ Start a pool of crawler processes """

        print 'Initializing',self.nprocs,'crawlers...'

        for i in range(self.nprocs):
            # Make a new instance
            crawler = EIIICrawler(task_queue = self.task_queue,
                                  value_dict = self.return_dict,
                                  state = self.state)
            log.info("Initialized Crawler ", crawler.id)
            self.instances.append(crawler)
            crawler.start()
        print 'Initialized',self.nprocs,'crawlers.'
        # Turn console logging off.
        log.setConsole(False)

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

        # Set task id
        config_dict['task_id'] = ctl.id_
        # Push the task
        task = (urls, config_dict)
        self.task_queue.put(task)
        # Increment tasks
        self.ntasks += 1
        
        return ctl.id_

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
            # print 'Client waiting...',task_id,'...'
            time.sleep(10)

        return_data = self.return_dict[task_id]
        url_graph = return_data['graph']
        stats_dict = return_data['stats']
        error_msg = return_data.get('error', '')
        
        if len(url_graph) <= 1:
            print 'URL graph is empty'
            print 'Fatal error message is =>',error_msg
        
        try:
            url_graph = fix_url_graph(url_graph)
        except Exception, e:
            log.error(traceback.format_exc())
        
        result = { 'result': make_directed_graph(url_graph),
                 'error': error_msg,
                 'stats': stats_dict,
                 '__type__': "crawler-result"}

        # Clean the data from the result dict
        try:
            del self.return_dict[task_id]
        except KeyError, e:
            print 'Could not remove data for',task_id,'from crawler shared dictionary.'
        
        return (result,self.load(None))

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
        
            return { 'result': make_directed_graph(url_graph),
                     'stats': stats_dict,
                     '__type__': "crawler-result"}
        else:
            print 'No result found for task',taskid
            return {}
    
    def load(self, ctl):
        """
        Returns a number. Ranges from 0 - 100. 0 means crawlers are idling,
        100 means all crawlers are active.
    
        The chance of getting a crawl call is proportional to how much below
        100 it is compared to the other servers in rotation.
        """

        # Load => # of active crawlers/# of nprocs
        nactive = len(filter(lambda x: x==1, self.state.values()))
        print '# active/# procs =>',nactive,'=>',self.nprocs
        return int(100.0*nactive/self.nprocs)
        
          
if __name__ == "__main__":
    crawler_rules = {'max-pages': [(['text/html', 'application/xhtml+xml', 'application/xml'], 50)],
                     'scoping-rules': [('+', '^https?://utt\\.tingtun\\.no')], 'min-crawl-delay': 2,
                     'size-limits': [(['text/html', 'application/xhtml+xml', 'application/xml'], 500)],
                     'seeds': ['http://docs.python.org/library/'], 'obey-robotstxt': 'false',
                     'loglevel': 'debug'}

    # You can given --nprocs as a command-line argument
    #
    # An equivalent number (or greater) of processes need to be
    # started on the tt-rpc server for proper 1:1 scaling.
    parser = argparse.ArgumentParser(description='EIII crawler server')
    parser.add_argument('--nprocs', dest='nprocs', default=10,type=int,
                        help='Number of crawler processes to start')
    parser.add_argument('--port', dest='port', default=8910,type=int,
                        help='Port number on which to listen')
    parser.add_argument('--debug', dest='loglevel', const='debug',
            default='info', nargs='?', help='Enable debug logging')
    parser.add_argument('--bus', dest='bus_uri', default=None, type=str,
                        help='URI to bus to register on.')
    parser.add_argument('--bindaddr', dest='bind_addr', default='127.0.0.1', type=str,
                        help='IP address on which to listen.')
    args = parser.parse_args()
    print 'Number of parallel crawler processes set to',args.nprocs
    print 'Starting crawler server on port',args.port,'...'
    if args.loglevel is 'debug':
        print 'Enabled debug messages.'
    if args.bus_uri:
        print 'Will register on bus at', args.bus_uri

    log.setLevel(args.loglevel)
    EIIICrawlerServer(nprocs=args.nprocs,loglevel=args.loglevel,
                      bus_uri=args.bus_uri, port=args.port, bind_addr=args.bind_addr
                     ).listen("tcp://%s:%d" % (args.bind_addr, args.port), nprocs=args.nprocs*2)


    
