""" Sample client for demonstrating how to talk with
the crawler server """

from ttrpc.client import *

crawler_rules = {'max-pages': [(['text/html', 'application/xhtml+xml', 'application/xml'], 50)],
                 'scoping-rules': [('+', '^https?://utt\\.tingtun\\.no')], 'min-crawl-delay': 2,
                 'size-limits': [(['text/html', 'application/xhtml+xml', 'application/xml'], 500)],
                 'seeds': ['https://docs.python.org/2/library/'], 'obey-robotstxt': False,
                 'loglevel': 'debug'}

# Wait for 3 hours for crawl to end.
proxy = TTRPCProxy('tcp://localhost:8910', retries=1,
                   timeout=10800*1000)

# print dir(proxy.poller)
crawl_graph = proxy.crawl(crawler_rules)
print 'Crawl graph=>',crawl_graph['result']
print
print 'Crawl stats=>',crawl_graph['stats']

# print proxy.load()

