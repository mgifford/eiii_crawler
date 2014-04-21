from ttrpc.server import SimpleTTRPCServer
import time
import logger
import utils
import sys
from eiii_crawler import EIIICrawler, log
from crawlerbase import CrawlerStats

class EIIICrawlerServer(SimpleTTRPCServer):
    """ EIII crawler server obeying the tt-rpc protocol """

    _logger = log
    
    def _ping(self, what):
        """ Private methods are not exposed. """
        return what

    def ping(self, ctl):
        return self._ping("pong")

    def crawl(self, ctl, crawler_rules):
        """ Accepts a crawler control object, dictionary of
        crawler rules and start crawling """

        print 'Starting crawl ...'
        # Map crawler rules into internal Rules.
        config_dict = utils.convert_config(crawler_rules)
        urls = config_dict.get('urls')

        if urls==None:
            print 'No URLs given!'
            sys.exit(1)

        crawler = EIIICrawler(urls, cfgfile='')
        # Update config with the configuration values
        crawler.config.__dict__.update(config_dict)
        crawler.config.save('crawl.json')

        crawler.crawl()
        
        # Wait for some time
        time.sleep(10)

        stats = CrawlerStats()
        
        while crawler.work_pending():
            time.sleep(5)
            # Update status
            if ctl: ctl.setStatus(str(stats.get_num_urls()),
                                  str(stats.get_crawl_url_rate()))
                

        crawler.eventr.publish(self, 'crawl_ended')        
        log.info('Crawl done.')

        # print self.url_graph
        stats.publish_stats()      

    def load(self, ctl):
        """
        Returns a number. If it's above 100, it's overloaded. The
        chance of getting a crawl call is proportional to how much below
        100 it is compared to the other servers in rotation.

        """

        # Keeping this for the time being.
        return int(float(open("/proc/loadavg").read().split()[0]) * 100)

          
if __name__ == "__main__":
    crawler_rules = {'max-pages': [(['text/html', 'application/xhtml+xml', 'application/xml'], 6000)],
                     'scoping-rules': [('+', '^https?://utt\\.tingtun\\.no')], 'min-crawl-delay': 2,
                     'size-limits': [(['text/html', 'application/xhtml+xml', 'application/xml'], 500)],
                     'seeds': ['http://www.tingtun.no'], 'obey-robotstxt': 'false'}

    port=8910
    print 'Starting crawler server on port',port,'...'
    # EIIICrawlerServer().crawl(None, crawler_rules)
    EIIICrawlerServer().listen("tcp://*:%d" % port)

    
