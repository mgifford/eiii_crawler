"""
Circuit breaker - Detect crawl loops and break them.

This is used to come out of a series of URLs sharing the same pattern
typically produced from a single parent URL or a few parent URLs.

"""

import urlparse
import re

from eiii_crawler.crawlerevent import subscribe
from eiii_crawler.crawlerbase import CrawlerConfig
from eiii_crawler import utils

# Dictionary mapping dynamic URL regexes to their hit counts
__regexes__ = {}
# Dictionary mapping dynamic URL regex to their total URLs count
__regexurls__ = {}

# Default logging object
log = utils.get_default_logger()

# Global configuration
# These are in this case,
# 1. Min # of URLs of a specific pattern to be found before threshod is imposed
# 2. The actual threshold of percentage of such URLs over # of total URLs (from the
# time the rule has been created). The dynamic rules are created only after the threshold
# is hit.

min_hits, threshold = 10, 20.0

def set_config(**kwargs):
    """ Set configuration for the plugin """

    for key,value in kwargs.items():
        # Set globally
        log.info("\tSetting configuration",key,"to",value)
        globals()[key] = value
    
@subscribe('download_complete')
def check_circuit(event):
    """ Check for circuits (loops) in URL patterns """

    url = event.params.get('url')
    # print 'Checking for circuit',url,'...'

    # Parse the URL
    urlp = urlparse.urlparse(url)
    # Take the query params
    query_p = urlp.query

    if query_p:
        # Check against any of the existing regexes
        # Split and normalize the query parameter
        query_p_norm = '&'.join(map(lambda x:x.strip(), query_p.split('&')))
        hit = False
        
        for regex in __regexes__:
            # Increase count of regex URL dictionary
            __regexurls__[regex] += 1
            # Check for a match
            if regex.match(query_p_norm):
                # print 'Matching regex =>',regex.pattern
                # Add a hit for this regex
                __regexes__[regex] += 1
                hit = True
                break

        # If no hit found - make an entry for this URLs query parameter in the regex dictionary
        if not hit:
            # print 'Making entry for URL',url,'in regex dictionary...'
            regex_s='\&'.join(map(lambda x:x.strip().split('=')[0] + '\=[a-zA-Z_0-9]+', query_p.split('&')))
            regex = re.compile(regex_s)
            # Insert entry into regex dictionary
            __regexes__[regex] = 0
            # Make 1 entry in regex URL dictionary
            __regexurls__[regex] = 1
        else:
            # Hit - check percentage - if >=20% of total URLs seen so far, mark this as
            # a repeating template. Also do this only after at least 10 actual hits
            count, total = __regexes__[regex], __regexurls__[regex]
            
            if (total>=min_hits) and 100.0*count/total >= threshold:
                # print 'Template hit for regex =>',regex.pattern, count, total
                # Append this rule to exclude of crawler config
                # Prefix a .* before the rule pattern to be a catch-all on prefix
                rule = '.*' + regex.pattern
                config = CrawlerConfig.getInstance()
                if rule not in config._url_dynamic_exclude_rules:
                    log.info('Hit threshold. Creating dynamic rule to exclude',regex.pattern,'...')
                    config._url_dynamic_exclude_rules.append(rule)
                    # print '\tnew exclusion rules=>',config._url_exclude_rules
                    # Drop this rule from the dictionary
                    del __regexurls__[regex]
                    del __regexes__[regex]
            
    else:
        # print 'No query param found, not doing anything'
        pass
