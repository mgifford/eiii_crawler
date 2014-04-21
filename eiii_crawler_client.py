""" Sample client for demonstrating how to talk with
the crawler server """

from ttrpc.client import *

crawler_rules = {'max-pages': [(['text/html', 'application/xhtml+xml', 'application/xml'], 6000)],
                 'scoping-rules': [('+', '^https?://utt\\.tingtun\\.no')], 'min-crawl-delay': 2,
                 'size-limits': [(['text/html', 'application/xhtml+xml', 'application/xml'], 500)],
                 'seeds': ['http://www.tingtun.no'], 'obey-robotstxt': 'false'}

proxy = TTRPCProxy('tcp://localhost:8910')
proxy.crawl(crawler_rules)
# print proxy.load()

