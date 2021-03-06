""" Module containing classes that implement the CrawlerUrlData
class in different ways """

# Right now there is just one implementation - the caching URL data
# implementation using helper methods from urlhelper.

import crawlerbase
import hashlib
import zlib
import os
import re
import httplib
import time

from eiii_crawler import urlhelper
from eiii_crawler import utils

from eiii_crawler.crawlerscoping import CrawlerScopingRules

# Default logging object
log = utils.get_default_logger()
# HTTP refresh headers
http_refresh_re = re.compile(r'\s*\d+\;\s*url\=([^<>]*)', re.IGNORECASE)

class CachingUrlData(crawlerbase.CrawlerUrlData):
    """ Caching URL data which implements caching of the downloaded
    URL data locally and supports HTTP 304 requests """

    # REFACTORME: This class does both downloading and caching.
    # The proper way to do this is to derive a class which does
    # only downloading, another which does caching and then
    # inherit this as a mixin from both (MI).
    
    def __init__(self, url, parent_url, content_type, config):
        super(CachingUrlData, self).__init__(url, parent_url, content_type, config)
        self.orig_url = self.url
        self.headers = {}
        self.content = ''
        self.content_length = 0
        # Given content-type if any
        self.given_content_type = content_type
        # Download status
        # True -> Success
        # False -> Failed
        self.status = False
        self.content_type = 'text/html'
        
    def get_url_store_paths(self):
        """ Return a 3-tuple of paths to the URL data and header files
        and their directory """

        # Let us write against original URL
        # Always assume bad Unicode
        urlhash = hashlib.md5(self.orig_url.encode('latin1')).hexdigest()
        # First two bytes for folder, next two for file
        folder, sub_folder, fname = urlhash[:2], urlhash[2:4], urlhash[4:]
        # Folder is inside 'store' directory
        dirpath = os.path.expanduser(os.path.join(self.config.storedir, folder, sub_folder))
        # Data file
        fpath = os.path.expanduser(os.path.join(dirpath, fname))
        # Header file
        fhdr = fpath + '.hdr'

        return (fpath, fhdr, dirpath)
        
    def write_headers_and_data(self):
        """ Save the headers and data for the URL to the local store """

        if self.config.flag_storedata:
            fpath, fhdr, dirpath = self.get_url_store_paths()
            # Write data to fpath
            # Write data ONLY if either last-modified or etag header is found.
            dhdr = dict(self.headers)
            lmt, etag = dhdr.get('last-modified'), dhdr.get('etag')
            
            try:
                with utils.ignore(): os.makedirs(dirpath)

                # Issue http://gitlab.tingtun.no/eiii/eiii_crawler/issues/412
                # Always save starting URL.
                # Hint - parent_url is None for starting URL.
                if ((self.parent_url == None) or (lmt != None) or (etag != None)) and self.content:
                    open(fpath, 'wb').write(zlib.compress(self.content))
                    log.info('Wrote URL content to',fpath,'for URL',self.url)

                if self.headers:
                    # Add URL to it
                    self.headers['url'] = self.url
                    open(fhdr, 'wb').write(zlib.compress(str(dict(self.headers))))
                    log.info('Wrote URL headers to',fhdr,'for URL',self.url)                    
            except Exception, e:
                # raise
                log.error("Error in writing URL data for URL",self.url)
                log.error("\t",str(e))

    def make_head_request(self, headers):
        """ Make a head request with header values (if-modified-since and/or etag).
        Return True if data is up-to-date and False otherwise. """

        lmt, etag = headers.get('last-modified'), headers.get('etag')
        if lmt != None or etag != None:
            req_header = {}
            if lmt != None and self.config.flag_use_last_modified:
                req_header['if-modified-since'] = lmt
            if etag != None and self.config.flag_use_etags:
                req_header['if-none-match'] = etag

            try:
                # print 'Making a head request =>',self.url
                fhead = urlhelper.head_url(self.url, headers=req_header,
                                           verify = self.config.flag_ssl_validate)

                # Status code is 304 ?
                if fhead.status_code == 304:
                    return True
            except urlhelper.FetchUrlException, e:
                pass
        else:
            log.debug("Required meta-data headers (lmt, etag) not present =>", self.url)

        # No lmt or etag or URL is not uptodate
        return False
        
    def get_headers_and_data(self):
        """ Try and retrieve data and headers from the cache. If cache is
        up-to-date, this sets the values and returns True. If cache is out-dated,
        returns False """

        if self.config.flag_usecache:
            fpath, fhdr, dirpath = self.get_url_store_paths()

            fpath_f = os.path.isfile(fpath)
            fhdr_f = os.path.isfile(fhdr)
            
            if fpath_f and fhdr_f:
                try:
                    content = zlib.decompress(open(fpath).read())
                    headers = eval(zlib.decompress(open(fhdr).read()))

                    if self.make_head_request(headers):
                        # Update URL from cache
                        self.url = self.headers.get('url', self.url)
                        
                        log.info(self.url, "==> URL is up-to-date, returning data from cache")

                        self.content = content
                        self.headers = headers

                        self.content_type =  urlhelper.get_content_type(self.url, self.headers)
                        
                        eventr = crawlerbase.CrawlerEventRegistry.getInstance()                 
                        # Raise the event for retrieving URL from cache
                        eventr.publish(self, 'download_cache',
                                       message='URL has been retrieved from cache',
                                       code=304,
                                       event_key=self.url,                                     
                                       params=self.__dict__)                    

                        return True
                except Exception, e:
                    log.error("Error in getting URL headers & data for URL",self.url)
                    log.error("\t",str(e))
            else:
                if not fpath_f:
                    log.debug("Data file [%s] not present =>" % fpath, self.url)
                if not fhdr_f:
                    log.debug("Header file [%s] not present =>" % fhdr, self.url)                    

        return False
        
    def build_headers(self):
        """ Build headers for the request """

        # User-agent is always sent
        headers = {'user-agent': self.useragent}
        for hdr in self.config.client_standard_headers:
            val = getattr(self.config, 'client_' + hdr.lower().replace('-','_'))
            headers[hdr] = val

        return headers

    def pre_download(self, crawler, parent_url=None):
        """ Steps to be executed before actually going ahead
        and downloading the URL """

        if self.get_headers_and_data():
            self.status = True
            # Obtained from cache
            return True

        eventr = crawlerbase.CrawlerEventRegistry.getInstance()
        
        try:
            # If a fake mime-type only do a HEAD request to get correct URL, dont
            # download the actual data using a GET.
            if self.given_content_type in self.config.client_fake_mimetypes or \
                   any(map(lambda x: self.given_content_type.startswith(x),
                           self.config.client_fake_mimetypes_prefix)):              
                log.info("Making a head request",self.url,"...")
                fhead = urlhelper.head_url(self.url, headers=self.build_headers())
                log.info("Obtained with head request",self.url,"...")

                self.headers = fhead.headers
                # If header returns 404 then skip this URL
                if fhead.status_code not in range(200, 300):
                    log.error('Error head requesting URL =>', fhead.url,"status code is",fhead.status_code)
                    return False
                
                if self.url != fhead.url:
                    # Flexi scope - no problem
                    # Allow external domains only for flexible site scope
                    print "SCOPE =>", self.config.site_scope
                    if self.config.site_scope == 'SITE_FLEXI_SCOPE':
                        self.url = fhead.url
                        log.info("URL updated to",self.url)                     
                    else:
                        scoper = CrawlerScopingRules(self.config, self.url)
                        if scoper.allowed(fhead.url, parent_url, redirection=True):
                            self.url = fhead.url
                            log.info("URL updated to",self.url)
                        else:
                            log.extra('Site scoping rules does not allow URL=>', fhead.url)
                            return False                            

                self.content_type =  urlhelper.get_content_type(self.url, self.headers)

                # Simulate download event for this URL so it gets added to URL graph
                # Publish fake download complete event          
                eventr.publish(self, 'download_complete_fake',
                               message='URL has been downloaded fakily',
                               code=200,
                               params=self.__dict__)

                self.status = False
                return True
        except urlhelper.FetchUrlException, e:
            log.error('Error downloading',self.url,'=>',str(e))
            # FIXME: Parse HTTP error string and find out the
            # proper code to put here if HTTPError.
            eventr.publish(self, 'download_error',
                           message=str(e),
                           is_error = True,
                           code=0,
                           params=self.__dict__)

        return False         

    def download(self, crawler, parent_url=None, download_count=0):
        """ Overloaded download method """

        eventr = crawlerbase.CrawlerEventRegistry.getInstance()

        index, follow = True, True
        
        ret = self.pre_download(crawler, parent_url)
        if ret:
            # Satisfied already through cache or fake mime-types
            return ret

        try:
            log.debug("Waiting for URL",self.url,"...")
            freq = urlhelper.get_url(self.url, headers = self.build_headers(),
                                     content_types=self.config.client_mimetypes + self.config.client_extended_mimetypes,
                                     max_size = self.config.site_maxrequestsize*1024*1024,
                                     verify = self.config.flag_ssl_validate
                                     )
            log.debug("Downloaded URL",self.url,"...")          

            self.content = freq.content
            self.headers = freq.headers

            # Initialize refresh url
            mod_url = refresh_url = self.url
            hdr_refresh = False
            
            # First do regular URL redirection and then header based.
            # Fix for issue #448, test URL: http://gateway.hamburg.de
            if self.url != freq.url:
                # Modified URL
                mod_url = freq.url

            # Look for URL refresh headers
            if 'Refresh' in self.headers:
                log.debug('HTTP Refresh header found for',self.url)
                refresh_val = self.headers['Refresh']
                refresh_urls = http_refresh_re.findall(refresh_val)
                if len(refresh_urls):
                    hdr_refresh = True
                    # This could be a relative URL
                    refresh_url = refresh_urls[0]
                    # Build the full URL
                    mod_url = urlhelper.URLBuilder(refresh_url, mod_url).build()
                    log.info("HTTP header Refresh URL set to",mod_url)
                
            # Is the URL modified ? if so set it 
            if self.url != mod_url:
                # Flexi scope - no problem
                # Allow external domains only for flexible site scope
                # print 'Scope =>',self.config.site_scope, parent_url
                if self.config.site_scope == 'SITE_FLEXI_SCOPE':
                    self.url = mod_url
                    log.info("URL updated to", mod_url)                
                else:
                    scoper = CrawlerScopingRules(self.config, self.url)
                    status = scoper.allowed(mod_url, parent_url, redirection=True)
                    # print 'SCOPER STATUS =>',status,status.status
                    if status:
                        self.url = mod_url
                        log.info("URL updated to",self.url)
                    else:
                        log.extra('Site scoping rules does not allow URL=>', mod_url)
                        return False

                # If refresh via headers, we need to fetch this as content as it
                # is similar to a URL redirect from the parser.
                if hdr_refresh:
                    log.info('URL refreshed via HTTP headers. Downloading refreshed URL',mod_url,'...')
                    parent_url, self.url = self.url, mod_url
                    # NOTEME: The only time this method calls itself is here.
                    # We set the URL to modified one and parent URL to the current one
                    # and re-download. Look out for Buggzzzies here.
                    return self.download(crawler, parent_url, download_count=download_count+1)

            # Add content-length also for downloaded content
            self.content_length = max(len(self.content),
                                      self.headers.get('content-length',0))

            self.content_type =  urlhelper.get_content_type(self.url, self.headers)
            # requests does not raise an exception for 404 URLs instead
            # it is wrapped into the status code

            # Accept all 2xx status codes for time being
            # No special processing for other status codes
            # apart from 200.

            # NOTE: requests library handles 301, 302 redirections
            # very well so we dont need to worry about those codes.
            
            # Detect pages that give 2xx code WRONGLY when actual
            # code is 404.
            status_code = freq.status_code
            if self.config.flag_detect_spurious_404:
                status_code = urlhelper.check_spurious_404(self.headers, self.content, status_code)

            if status_code in range(200, 300):
                self.status = True
                eventr.publish(self, 'download_complete',
                               message='URL has been downloaded successfully',
                               code=200,
                               event_key=self.url,
                               params=self.__dict__)
                
            elif status_code in range(500, 1000):
                # There is an error but if we got data then fine - Fix for issue #445
                self.status = True
                eventr.publish(self, 'download_complete',
                               message='URL has been downloaded but with an error',
                               code=500,
                               event_key=self.url,
                               params=self.__dict__)
            else:
                log.error("Error downloading URL =>",self.url,"status code is ", status_code)
                eventr.publish(self, 'download_error',
                               message='URL has not been downloaded successfully',
                               code=freq.status_code,
                               params=self.__dict__)

            self.write_headers_and_data()
            freq.close()
        except urlhelper.FetchUrlException, e:
            log.error('Error downloading',self.url,'=>',str(e))
            # FIXME: Parse HTTP error string and find out the
            # proper code to put here if HTTPError.
            eventr.publish(self, 'download_error',
                           message=str(e),
                           is_error = True,
                           code=0,
                           params=self.__dict__)
        except httplib.IncompleteRead, e:
            log.error("Error downloading",self.url,'=>',str(e))
            # Try 1 more time
            time.sleep(1.0)
            if download_count == 0:
                log.info('Retrying download for',self.url,'...')
                return self.download(crawler, parent_url, download_count=download_count+1)
            else:
                # Raise error
                eventr.publish(self, 'download_error',
                               message=str(e),
                               is_error = True,
                               code=0,
                               params=self.__dict__)
            
        except (urlhelper.InvalidContentType, urlhelper.MaxRequestSizeExceeded), e:
            log.error("Error downloading",self.url,'=>',str(e))
            eventr.publish(self, 'download_error',
                           message=str(e),
                           is_error = True,
                           code=0,
                           params=self.__dict__)
        return True

            
    def get_data(self):
        """ Return the data """

        return self.content

    def get_headers(self):
        """ Return the headers """
        return self.headers

    def get_url(self):
        """ Return the downloaded URL. This is same as the
        passed URL if there is no modification (such as
        forwarding) """

        return self.url 

    def get_content_type(self):
        """ Return the content-type """

        # URL might have been,
        # 1. Actually downloaded
        # 2. Obtained from cache
        # 3. Faked (head request)
        # Make sure self.content_type is up2date
        # in all 3 cases.
        return self.content_type
