""" Classes keeping statistics of crawl """

import datetime

import utils
from crawlerevent import CrawlerEventRegistry

# Default logging object
log = utils.get_default_logger()

class CrawlerStats(object):
    """ Class keeping crawler statistics such as total URLs downloaded,
    total URLs parsed, total time taken etc """

    # This class is a Singleton
    __metaclass__ = utils.SingletonMeta
    
    def __init__(self):
        self.reset()
        # Subscribe to events
        eventr = CrawlerEventRegistry.getInstance()
        eventr.subscribe('download_complete', self.update_total_urls_downloaded)
        eventr.subscribe('download_cache', self.update_total_urls_cache)       
        eventr.subscribe('download_error', self.update_total_urls_error)
        eventr.subscribe('url_obtained', self.update_total_urls)
        eventr.subscribe('url_filtered', self.update_total_urls_skipped)
        eventr.subscribe('crawl_started', self.mark_start_time)
        eventr.subscribe('crawl_ended', self.mark_end_time)                     
        pass

    def reset(self):
        """ Reset the stats """

        # Number of total URLs crawled
        self.num_urls = 0
        # Number of total URLs downloaded
        self.num_urls_downloaded = 0
        # Number of URLs skipped (due to rules etc)
        self.num_urls_skipped = 0
        # Number of URLs with error
        self.num_urls_error = 0
        # Number of urls not found (404 error)
        self.num_urls_notfound = 0
        # Number of URLs retrieved from cache
        self.num_urls_cache = 0

        # Time
        # Start time-stamp
        self.start_timestamp = ''
        # End time-stamp
        self.end_timestamp = ''
        # Time taken for total crawl
        self.crawl_time = 0
        # Time taken for download
        self.download_time = 0
        # Total sleep time
        self.sleep_time = 0
        
    def update_total_urls(self, event):
        """ Update total number of URLs """

        # NOTE: This also includes duplicates, URLs with errors - everything.
        self.num_urls += 1

    def update_total_urls_cache(self, event):
        """ Update total number of URLs retrieved from cache """

        self.num_urls_cache += 1
        # log.debug('===> Number of URLs from cache <===',self.num_urls_cache)
        
    def update_total_urls_downloaded(self, event):
        """ Update total number of URLs downloaded """

        self.num_urls_downloaded += 1
        # log.debug('===> Number of URLs downloaded <===',self.num_urls_downloaded)

    def update_total_urls_skipped(self, event):
        """ Update total number of URLs skipped """

        self.num_urls_skipped += 1
        # Skipped URLs have to be added to total URLs
        # since these don't get into the queue
        self.num_urls += 1

    def update_total_urls_error(self, event):
        """ Update total number of URLs that failed to download with error """

        self.num_urls_error += 1        
        if event.code == 404:
            self.num_urls_notfound += 1

    def mark_start_time(self, event):
        """ Mark starting time of crawl """

        self.start_timestamp = datetime.datetime.now().replace(microsecond=0)

    def mark_end_time(self, event):
        """ Mark end time of crawl """

        self.end_timestamp = datetime.datetime.now().replace(microsecond=0)
        self.crawl_time = str(self.end_timestamp - self.start_timestamp)

    def get_crawl_url_rate(self):
        """ Return crawling rate in terms of # URLs/sec """

        d = datetime.datetime.now()
        delta = (d - self.start_timestamp).total_seconds()
        return 1.0*(self.num_urls_downloaded + self.num_urls_cache)/delta

    def get_num_urls(self):
        """ Get URLs crawled so far """

        return self.num_urls_downloaded + self.num_urls_cache
    
    def publish_stats(self):
        """ Publish crawl stats """

        log.info('')
        log.info("oxoxox BEGIN CRAWL STATISTICS xoxoxo")
        log.justlog("Start Timestamp",self.start_timestamp, justify=40)
        log.justlog("End Timestamp",self.end_timestamp, justify=40)
        log.justlog("Crawl Time",str(self.crawl_time), justify=40)
        log.justlog("# URLs",self.num_urls, justify=40)
        log.justlog("# URLs downloaded",self.num_urls_downloaded, justify=40)
        log.justlog("# URLs with error",self.num_urls_error, justify=40)
        log.justlog("# URLs 404",self.num_urls_notfound, justify=40)
        log.info("oxoxox END CRAWL STATISTICS xoxoxo")
        log.info('')
        
