#!/usr/bin/env python

import sys
import simplejson as json
import eiii_crawler_server as c
import psycopg2
import psycopg2.extras
psycopg2.extras.register_uuid()
import uuid

"""
Use this script to reconstruct the crawler_result from the file dumped by the crawler.
Supply the file with the crawler stats as well as the site_result_uid for the site
result you want to update.

An additional, third parameter can be supplied to force overwriting of preexisting
crawler result.

Example:

% eiii-crawler/eiii_crawler/reconstruct_crawl_results.py \
        /home/tingtun/.eiii/crawler/stats/a3282a48-08eb-4595-a5fc-ea2670de3ad3.json \
        '4b237224-039e-47a5-99a2-cb2dd12634d9'

UPDATE 1
commit

"""

def make_url_graph(crawl_graph):
     return c.make_directed_graph(c.fix_url_graph(crawl_graph))

if __name__ == "__main__":
    crawler_stats = sys.argv[1]
    site_result_uid = uuid.UUID(sys.argv[2])
    force = len(sys.argv) == 4

    conn=psycopg2.connect("dbname=eiii")
    cur=conn.cursor()

    # sanity check
    cur.execute("""SELECT site_result_uid
                   FROM site_results
                   WHERE site_result_uid=%s
                   AND crawler_result IS NULL""",
                (site_result_uid,))

    if cur.rowcount is 0 and not force:
        print "that site result already has a crawl result."
        exit(1);

    stats_dict = json.loads(open(crawler_stats).read())
    url_graph = make_url_graph(stats_dict['url_graph'])
    crawler_result = json.dumps({'stats': stats_dict, 'result': url_graph})
    # print crawler_result

    cur.execute("""UPDATE site_results
                    SET crawler_result=%s
                    WHERE site_result_uid=%s""",
                (crawler_result,site_result_uid))
    print cur.statusmessage
    # sanity check, again
    if cur.statusmessage == 'UPDATE 1':
        print 'commit'
        conn.commit()
    else:
        print 'rollback'
        conn.rollback()
    cur.close()
    conn.close()

