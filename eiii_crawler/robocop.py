# -- coding: utf-8
""" Robots.txt parsing and META robots tags checking """

import urlparse
import re

import eiii_crawler.urlhelper as urlhelper

___author__ = "Anand B Pillai"
__maintainer__ = "Anand B Pillai"
__version__ = "0.1"

class Robocop(object):
    """ Robots.txt parser for websites """

    meta_re = re.compile(r'\<meta\s+name=\"robots\"\s+content=\"([a-zA-Z,\s]+)\"\s*', re.IGNORECASE)
    
    def __init__(self, url=None, useragent=None, debug=False):

        # An instance of this class is meant to be persisted
        # against a crawler and used. Don't reinitialize this
        # everytime for fetching a URL as it will cost you
        # in terms of network fetches and is rather inefficient.
        # Intialize once and then use the "check" method. One
        # instance of this class can be used to check many sites
        self.ua = useragent
        # Compiled rules - dictionary with the site
        # as key and list of compiled rules as values
        self.rules = {}
        # Debug flag
        self.debug = debug
        if url: self.parse_site(url)
        
    def parse_site(self, url):
        """ Parse robots.txt rules for a site or URL """

        if self.debug:
            print 'Parsing site =>',url

        site = urlhelper.get_website(url, scheme=True)
        # Site without scheme
        site_nos = urlhelper.get_website(url, remove_www=False)
        # print 'Site no scheme =>',site_nos
        # print 'Robocop rules =>',self.rules
        # If rules already exist, don't parse again
        if self.rules.has_key(site_nos):
            # print "Robots.txt already parsed for site",site_nos
            return True, ''
        
        robots_url = site + '/robots.txt'

        try:
            # print 'Robots URL =>',robots_url
            # print 'Fetching URL =>',robots_url
            freq = urlhelper.fetch_url(robots_url, verify=True)
            content = freq.content.split('\n')
            self.parse_robotstxt(content, site_nos)
        except urlhelper.FetchUrlException, e:
            # Bug : This cause the frosta kommune slow crawl issue #422. The
            # robots.txt URL of this site was causing a lot of redirects
            # and finally failing, but we are not catching it. We need
            # to catch it and disable the robots.txt rules of the site
            # by adding a default allow-all rule.
            content = []
            # Allow all
            self.rules[site_nos] = []
            return False, e

        return True, ''

    def parse_robotstxt(self, content, site):
        """ Parse the robots.txt content """
        
        sitemap = ''
        site_rules, raw_rules = [], []
        
        useragent = '*'
        state = ''

        self.crawldelay = -1

        for line in content:
            # print 'LINE =>',line
            line = line.lower().strip()
            if line.startswith('#'):
                continue

            # Issue #442, drop inline comments in rules
            line = line.split('#')[0]
            
            if line.startswith('sitemap:'):
                # CHECKME: So ? This is not being used further or what...
                sitemap = ':'.join(line.split(':')[1:]).strip()
                continue
            # if line.lower().strip().startswith('user-agent:'):
            if 'user-agent' in line:
                useragent = ':'.join(line.split(':')[1:]).strip()
                # print 'user-agent:',useragent
                continue
            if 'disallow' in line.lower():
                state = 'disallow'
                # Do we have a disallow: / ?
            elif 'allow' in line.lower():
                state = 'allow'
            if (line.startswith('crawldelay:') or line.startswith('crawl-delay:')) and useragent in ('*',self.ua):
                # Remove any comments - Issue #439
                self.crawldelay = float(':'.join(line.split(':')[1:]).strip())
                continue

            # Bug: this will catch ALL content, even if the content
            # doesn't have the format of a robots.txt!
            # E.g: http://www.nord-odal.kommune.no/no/robots.txt
            if state == 'disallow' and useragent in ('*', self.ua):
                # print useragent,disallowallow,line#,rules
                rules = ':'.join(line.split(':')[1:]).strip().split()
                # print 'rules=>',rules
                for rule in rules:
                    # print 'Rule=>',rule
                    if rule.strip():
                        raw_rules.append(rule)

        # Compile the rules and map it to the site
        # print 'Rules =>',raw_rules
        # Issue #432 - Ignore default block-all rule if specific rules are present
        if '/' in raw_rules and len(raw_rules)>1:
            raw_rules.remove('/')

        for rule in raw_rules:
            # Ignore rules like .* _* etc i.e any rule ending with an asterik
            # Fix for issue #447
            if rule.strip().endswith('*'): continue
            
            if 'http' in rule:
                site_rules.append(rule + '.*')
            else:
                site_rules.append('https?://'+site+'/'+rule.lstrip('/')+'.*')

        # print 'Site rules =>',site_rules
        # rules_c = [re.compile(i.lower()) for i in site_rules]
        rules_c = []
        for i in site_rules:
            try:
                rules_c.append(re.compile(i.lower()))
            except:
                pass
            
        self.rules[site] = rules_c

    def x_robots_check(self, url, headers):
        """ Check X-Robots-Tag header """

        # Ref: https://developers.google.com/webmasters/control-crawl-index/docs/robots_meta_tag
        index, follow = True, True
        
        # X-robots ?
        x_robots = headers.get('x-robots-tag','').lower().strip()
        if x_robots:
            # Exclude any user-agent string
            pieces = x_robots.split(':')
            if len(pieces)==2:
                ua, x_robots = pieces
                # If ua is specified, no chance it is applicable to us
                # since we are not in the wild yet.
                return (True, True)

            if x_robots == 'none':
                # Cannot index or follow
                return (False, False)

            # If there is a user-agent specified 
            items = sorted(x_robots.split(','))

            if 'noindex' in items:
                # Don't index this URL
                index = False

            if 'nofollow' in items:
                follow = False

        return (index, follow)

    def check_meta(self, url, content=None):
        """ Check META robots tags and return a 2-tuple of
        booleans (can_index, can_follow) for the URL """

        if content==None:
            # Need to fetch the content
            try:
                # print 'Fetching URL (meta) =>',robots_url              
                freq = urlhelper.fetch_url(url, verify=True)
                content = freq.content
            except Exception, e:
                print 'Error fetching URL =>',str(e)
                # Return default
                return (True, True)

        # Look for meta robots tag
        meta_match = list(set(self.meta_re.findall(content)))
        if meta_match:
            # Split into 2
            try:
                rules = {x.strip():1 for x in meta_match[0].lower().split(',')}
                # print 'RULEZ =>',rules
                # return (rules[0] == 'index', rules[1] == 'follow')
                # Negative catch-all is better since we can have rules
                # like content="ALL"
                return (not 'noindex' in rules, not 'nofollow' in rules)
            except:
                pass

        # Default
        return (True, True)
        
    def can_fetch(self, url, content=None, meta=False):
        """ Check whether a URL is fine to be downloaded according
        to both robots.txt and (optional) META robots rules. The URL
        should belong to the site this instance is configured with for
        the check to make any sense. If META robots tag is to be checked
        it is a good idea to pass the content of the URL since this
        avoids another fetch of the URL """

        # Pick up the rules
        site = urlhelper.get_website(url, remove_www=False)

        # Prefix scheme in front of URL
        url = urlhelper.get_full_url(url)

        # Check meta if asked
        if meta: 
            return self.check_meta(url, content)[0]

        site_rules = self.rules.get(site, [])
        # print site_rules
        
        # Maybe no robots.txt ?
        if len(site_rules)==0:
            # Allow - default
            return True
        
        # Check against the rules
        return not any(rule.match(url) for rule in site_rules)
                  
if __name__ == '__main__':

    # These are unit-tests for robocop module.
    # Issue #442
    r = Robocop('http://kildare.ie', debug=True)
    assert(r.can_fetch('http://kildare.ie/contact/'))
    r.parse_site('https://www.epaslaugos.lt')
    
    assert(r.can_fetch('https://www.epaslaugos.lt/portal/business'))
    assert(not r.can_fetch('https://www.epaslaugos.lt/egovportal/business'))

    r.parse_site('http://www.askoy.kommune.no')
    # Check if can fetch
    assert(not r.can_fetch('www.askoy.kommune.no/cache/test'))
    assert(not r.can_fetch('www.askoy.kommune.no/logs/log1.log'))
    assert(r.can_fetch('www.askoy.kommune.no/pictres/1.jpg'))
    # Explicit parsing
    r.parse_site('http://www.metatags.info/')
    assert(r.can_fetch('http://www.metatags.info/meta_name_robots', meta=True))
    assert(not r.can_fetch('http://www.metatags.info/cpanel/testing'))
    # implicit parsing
    r.parse_site('www.robotstxt.org')
    assert(r.can_fetch('http://www.robotstxt.org/meta.html', meta=True))
    r.parse_site('www.aljazeera.net')   
    assert(r.can_fetch('http://www.aljazeera.net/news/healthmedicine'))
    assert(not r.can_fetch('http://www.aljazeera.net/ajamonitor'))

    # No robots.txt for this site
    r.parse_site('www.nord-odal.kommune.no')
    assert(r.can_fetch('http://www.nord-odal.kommune.no/test/'))
    assert(r.check_meta('http://www.vestnes.kommune.no/Modules/Default.aspx') == (True, True))
    assert(r.check_meta('http://www.chami.com/tips/internet/010198I.html') == (True, True))

    # One of few sites that disable crawl/index via meta robots
    r.parse_site('http://www.salangen.kommune.no/')
    assert(not r.can_fetch('http://www.salangen.kommune.no/', meta=True))

    r.parse_site('http://www.dsb.no')
    # Test for frosta kommune.no
    r.parse_site('http://www.frosta.kommune.no')
    assert(r.can_fetch('http://www.frosta.kommune.no/xyz/'))

    r.parse_site('http://www.trysil.kommune.no')
    assert(r.can_fetch('http://www.trysil.kommune.no/'))
    assert(not r.can_fetch('http://www.trysil.kommune.no/publishingimages/forms/'))     

    # Adding a test for this site where robocop crashed - Issue #433
    r.parse_site('http://www.arbetsformedlingen.se')
    assert(not r.can_fetch('http://www.arbetsformedlingen.se/sitevision/proxy/4.306228a513d6386d3d854dc.html'))
    # Issue #437 - Crawl delay as float.
    r.parse_site('http://www.gov.uk')

    # Issue #439 - comment after crawl delay
    r.parse_site('http://www.cm-palmela.pt/')

    print 'All tests passed.'
    
    
