""" Sample client for demonstrating how to talk with
the crawler server """

from ttrpc.client import *
import sys

crawler_rules = {'max-pages': [(['text/html', 'application/xhtml+xml', 'application/xml'], 50)],
                 'scoping-rules': [('+','^anand.*')], 'min-crawl-delay': 2,
                 'size-limits': [(['text/html', 'application/xhtml+xml', 'application/xml'], 500)],
                 'seeds': ['http://english.mofa.gov.qa/'], 'obey-robotstxt': False,
                 'loglevel': 'debug'}

# Wait for 3 hours for crawl to end.
proxy = TTRPCProxy('tcp://localhost:8910', retries=1,
                   timeout=10800*1000)

# Set seeds
crawler_rules['seeds'] = sys.argv[1:]
print crawler_rules['seeds']

# print dir(proxy.poller)
task_id = proxy.crawl(crawler_rules)
# Wait for results.
print 'Task_id=>',task_id
crawl_graph = proxy.poll(task_id)

print 'Crawl graph=>',crawl_graph['result']

