from ttrpc.server import SimpleTTRPCServer, UserError
import time
import datetime
from eiii_crawler import logger, utils
import sys
from eiii_crawler.eiii_crawler import EIIICrawler, log
from eiii_crawler.crawlerbase import CrawlerStats

class EIIICrawlerServer(SimpleTTRPCServer):
    """ EIII crawler server obeying the tt-rpc protocol """

    class _LoggerWrapper(object):
        def __getattr__(self, name):
            f = getattr(log, name)
            def wrapper(s, *args):
                return f(s % args)
            return wrapper

    _logger = _LoggerWrapper()

    def _packer_default(self, obj):
        strtypes = (datetime.datetime, datetime.date, datetime.timedelta)
        if any(isinstance(obj, x) for x in strtypes):
            return str(obj)
        return obj

    def ping(self, ctl):
        return "pong"

    def crawl(self, ctl, crawler_rules):
        """ Accepts a crawler control object, dictionary of
        crawler rules and start crawling """

        if ctl:
            ctl.setStatus("Starting crawl ...")
        print 'Starting crawl ...'
        # Map crawler rules into internal Rules.
        config_dict = utils.convert_config(crawler_rules)
        urls = config_dict.get('urls')

        if urls==None:
            print 'No URLs given!'
            raise UserError(0, "No URLs given!")

        # Set log level
        log.setLevel(crawler_rules.get('loglevel','info'))
        # print config_dict
        # sys.exit(0)

        # Set task id
        config_dict['task_id'] = ctl.id_
        crawler = EIIICrawler(urls, cfgfile='',fromdict=config_dict)
        # Update config with the configuration values
        crawler.config.save('crawl.json')

        crawler.crawl()
        
        # Wait for some time
        time.sleep(10)

        stats = crawler.stats
        
        while crawler.work_pending():
            time.sleep(5)
            # Update status
            if ctl: ctl.setStatus(str(stats.get_num_urls()) + ", " +
                                  str(stats.get_crawl_url_rate()))
                

        crawler.eventr.publish(self, 'crawl_ended')        
        log.info('Crawl done.')

        # print self.url_graph
        stats.publish_stats()      
        # Get stats object
        stats_dict = stats.get_stats_dict()
        
        # Get the graph
        url_graph = crawler.get_url_graph()
        # crawler.quit()
        
        # Keys => URLs, values => list of child URL tuples
        # (url, content_type)
        # print url_graph
        # print url_graph.keys()
        return { 'result': self.make_directed_graph(url_graph),
                 'stats': stats_dict,
                 '__type__': "crawler-result"}
                
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
    # EIIICrawlerServer().crawl(None, crawler_rules)
    EIIICrawlerServer().listen("tcp://*:%d" % port)

    
