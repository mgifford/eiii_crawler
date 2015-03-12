#!/usr/bin/env python

import sys
import simplejson as json
import eiii_crawler_server as c

"""
Use this script to reconstruct the crawler_result from the file dumped by the crawler.
The resulting output can be manually inserted to the DB.

Example:

% eiii-crawler/eiii_crawler/reconstruct_crawl_results.py \
        /home/tingtun/.eiii/crawler/stats/a3282a48-08eb-4595-a5fc-ea2670de3ad3.json \
        > crawler_result.json

% psql -deiii -c "update site_results set crawler_result=\$here_crawler_result\$$(<crawler_result.json)\$here_crawler_result\$ where site_result_uid='4b237224-039e-47a5-99a2-cb2dd12634d9';"

"""

def make_url_graph(crawl_graph):
     return c.make_directed_graph(c.fix_url_graph(crawl_graph))

if __name__ == "__main__":
    crawler_stats = sys.argv[1]
    stats_dict = json.loads(open(crawler_stats).read())
    url_graph = make_url_graph(stats_dict['url_graph'])
    crawler_result = json.dumps({'stats': stats_dict, 'result': url_graph})
    print crawler_result

