""" EIII web-crawler using Selenium web-driver """

import crawlerbase
from crawlerbase import CrawlerEventRegistry
import eiii_crawler
import sys, os
import urlhelper
import time
import argparse
import logger
import utils
from selenium import webdriver
from selenium.webdriver.common.proxy import *

log = logger.getMultiLogger('eiii_crawler','crawl.log','crawl.err',console=True)

__version__ = '1.0a'
__author__ = 'Anand B Pillai'
__maintainer__ = 'Anand B Pillai'

class EIIISeleniumCrawlerUrlData(eiii_crawler.EIIICrawlerUrlData):
    """ Class representing downloaded data for a URL.
    The data is downloaded using selenium web driver """

    def __init__(self, url, parent_url, config, driver):
        super(EIIISeleniumCrawlerUrlData, self).__init__(url, parent_url, config)
        self.driver = driver
        
    def download(self, crawler, parent_url=None):
        """ Overloaded download method """

        eventr = CrawlerEventRegistry.getInstance()

        index, follow = True, True

        # Doesn't seem to work for Selenium crawler
        if self.get_headers_and_data():
            # Obtained from cache
            return True
        
        try:
            # freq = urlhelper.get_url(self.url, headers = self.build_headers())
            self.driver.get(self.url)
            
            self.content = self.driver.page_source.encode('ascii','xmlcharrefreplace')
            # Encode it 
            self.headers = {}

            # Page title
            title = self.driver.title

            # Check if error
            urlhelper.check_page_error(title, self.content)
            
            # Is the URL modified ? if so set it 
            if self.url != self.driver.current_url:
                self.url = self.driver.current_url
                log.extra("URL updated to",self.url)
            
            # Add content-length also for downloaded content
            self.content_length = len(self.content)

            self.content_type =  urlhelper.get_content_type(self.url, self.headers)
            # Selenium does not give us headers or status code!
            # Need to assume 200 unless the page content indicates something else
            
            status_code = 200

            # Detect pages that give 2xx code WRONGLY when actual
            # code is 404.          
            if self.config.flag_detect_spurious_404:
                status_code = urlhelper.check_spurious_404_title(title, status_code)
            
            if status_code in range(200, 300):
                eventr.publish(self, 'download_complete',
                               message='URL has been downloaded successfully',
                               code=200,
                               params=self.__dict__)
            else:
                log.error("Error downloading URL =>",self.url,"status code is ",freq.status_code)
                eventr.publish(self, 'download_error',
                               message='URL has not been downloaded successfully',
                               code=freq.status_code,
                               params=self.__dict__)

            self.write_headers_and_data()
        except urlhelper.FetchUrlException, e:
            log.error('Error downloading',self.url,'=>',str(e))
            # FIXME: Parse HTTP error string and find out the
            # proper code to put here if HTTPError.
            eventr.publish(self, 'download_error',
                           message=str(e),
                           is_error = True,
                           code=0,
                           params=self.__dict__)

        return True

            
class EIIISeleniumCrawlerQueuedWorker(eiii_crawler.EIIICrawlerQueuedWorker):
    """ EIII Crawler worker using a shared FIFO queue as
    the data structure that is used to share data """
    
    def __init__(self, config, manager):
        self.driver = manager.get_web_driver()
        super(EIIISeleniumCrawlerQueuedWorker, self).__init__(config, manager)

    def get_url_data_instance(self, url, parent_url=None):
        """ Make an instance of the URL data class
        which fetches the URL """

        return EIIISeleniumCrawlerUrlData(url, parent_url, self.config, self.driver)

    def parse(self, data, url):
        """ Parse the URL data and return an iterator over child URLs """

        urls = []
        
        parser = urlhelper.URLLister()

        try:
            log.debug("Parsing URL",url)
            parser.feed(data)
            parser.close()

            self.eventr.publish(self, 'url_parsed',
                                params=locals())
            
        except sgmllib.SGMLParseError, e:
            log.error("Error parsing data for URL =>",url)

        urls = list(set(parser.urls))

        # Has the base URL changed ?
        if parser.base_changed:
            url = parser.source_url
            
        return (url, urls)

class EIIISeleniumCrawler(eiii_crawler.EIIICrawler):
    """ EIII Web Crawler using Selenium web driver """

    def __init__(self, urls, cfgfile='config.json', fromdict={}, args=None):
        super(EIIISeleniumCrawler, self).__init__(urls, cfgfile, fromdict)
        # Web driver
        self.remote_driver = False      
        self.driver = self.make_web_driver(args)
        # With selenium we can have only 1 worker
        self.config.num_workers = 1
        # Driver mappings

    def get_web_driver(self):
        """ Return the web-driver instance """

        return self.driver
    
    def make_worker(self):
        """ Make a worker instance """

        return EIIISeleniumCrawlerQueuedWorker(self.config, self)
    
    def make_web_driver_with_proxy(self, args):
        """ Return the selenium web-driver instance that
        supports an (open) network proxy server """

        myProxy = self.config.network_proxy
        
        proxy = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': myProxy,
            'ftpProxy': myProxy,
            'sslProxy': myProxy,
            'noProxy': '' # set this value as desired
            })

        log.info('Using network proxy',myProxy,'...')
        # If remote driver specified, use it
        if args.remote:
            self.remote_driver = True
            log.info('Using remote driver at',args.remote)
            # for remote
            caps = webdriver.DesiredCapabilities.FIREFOX.copy()
            proxy.add_to_capabilities(caps)
            driver = webdriver.Remote("http://%s/wd/hub" % args.remote,
                                      desired_capabilities=caps)
            
        else:
            driver_name = args.driver
            log.info('Selected driver',driver_name)
            if driver_name != 'phantomjs':
                driver = getattr(webdriver, driver_name[0].upper() + driver_name[1:])(proxy=proxy)
            else:
                # Cannot use proxy 
                driver = getattr(webdriver, 'PhantomJS')()                              

        return driver
        
    def make_web_driver(self, args):
        """ Return the selenium web-driver instance """

        if self.config.network_proxy:
            return self.make_web_driver_with_proxy(args)

        if args:
            # If remote driver specified, use it
            if args.remote:
                self.remote_driver = True               
                log.info('Using remote driver at',args.remote)
                driver = webdriver.Remote("http://%s/wd/hub" % args.remote,
                                          webdriver.DesiredCapabilities.FIREFOX.copy())
            else:
                driver_name = args.driver
                log.info('Selected driver',driver_name)
                if driver_name != 'phantomjs':
                    driver = getattr(webdriver, driver_name[0].upper() + driver_name[1:])()
                else:
                    driver = getattr(webdriver, 'PhantomJS')()              
        else:
            driver = webdriver.Firefox()

        return driver

    def quit(self):
        print 'Quitting...'
        # If remote, close
        # If local, quit
        if self.remote_driver:
            self.driver.close()
        else:
            self.driver.close()         
            self.driver.quit()          
        
    @classmethod
    def parse_options(cls):
        """ Parse command line options """

        if len(sys.argv)<2:
            sys.argv.append('-h')

        parser = argparse.ArgumentParser(prog='eiii_selenium_webcrawler',description='Web-crawler for the EIII project using Selenium - http://eiii.eu')
        parser.add_argument('-v','--version',help='Print version and exit',action='store_true')
        parser.add_argument('-l','--loglevel',help='Set the log level',default='info',metavar='LOGLEVEL')
        parser.add_argument('-c','--config',help='Use the given configuration file',metavar='CONFIG',
                            default='config.json')
        parser.add_argument('-d','--driver',help='Use the given web-driver instance (options: firefox, chrome, opera, phantomjs)',default='firefox')
        parser.add_argument('-r','--remote',help='Connect to remote web driver (host:port)',metavar='REMOTE_DRIVER')
        parser.add_argument('urls', nargs='*', help='URLs to crawl')

        args = parser.parse_args()
        if args.version:
            print 'EIII web-crawler (selenium): Version',__version__
            sys.exit(0)

        if len(args.urls)==0:
            print 'No URLs given, nothing to do'
            sys.exit(1)

        # Supported browsers - firefox, chrome, opera, phantomjs since we run on *nix
        if args.driver.lower() not in ('firefox','chrome','opera','phantomjs'):
            print 'Invalid web driver',args.driver
            sys.exit(1)
            
        # Set log level
        if args.loglevel.lower() in ('info','warn','error','debug','critical','extra'):
            log.setLevel(args.loglevel)
        else:
            print 'Invalid log level',args.loglevel
            sys.exit(1)
            
        return args 

if __name__ == "__main__":
    # Run this as $ python eiii_crawler.py <url>
    # E.g: python eiii_crawler.py -l debug -c myconfig.json http://www.tingtun.no
    EIIISeleniumCrawler.main()
