# -- coding: utf-8
""" Robots.txt parsing and META robots tags checking """

import urlhelper
import urlparse
import re

___author__ = "Anand B Pillai"
__maintainer__ = "Anand B Pillai"
__version__ = "0.1"

class Robocop(object):
    """ Robots.txt parser for websites """

    meta_re = re.compile(r'\<meta\s+name=\"robots\"\s+content=\"([a-zA-Z,\s]+)\"\s*', re.IGNORECASE)
    
    def __init__(self, url=None, useragent=None):

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
        if url: self.parse_site(url)
        
    def parse_site(self, url):
        """ Parse robots.txt rules for a site or URL """
        
        site = urlhelper.get_website(url, scheme=True)
        # Site without scheme
        site_nos = urlhelper.get_website(url)
        # If rules already exist, don't parse again
        if self.rules.has_key(site_nos):
            # print "Robots.txt already parsed for site",site_nos
            return
        
        robots_url = site + '/robots.txt'

        try:
            # print 'Robots URL =>',robots_url
            freq = urlhelper.fetch_url(robots_url)
            content = freq.content.split('\n')
            self.parse_robotstxt(content, site_nos)
        except:
            content = []

    def parse_robotstxt(self, content, site):
        """ Parse the robots.txt content """
        
        sitemap = ''
        site_rules = []

        useragent = '*'
        state = ''

        self.crawldelay = -1

        for line in content:
            # print 'LINE =>',line
            line = line.lower().strip()
            if line.startswith('#'):
                continue            
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
            elif 'allow' in line.lower():
                state = 'allow'
            if (line.startswith('crawldelay:') or line.startswith('crawl-delay:')) and useragent in ('*',self.ua):
                self.crawldelay = int(':'.join(line.split(':')[1:]).strip())
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
                        if 'http' in rule:
                            site_rules.append(rule + '.*')
                        else:
                            site_rules.append('https?://'+site+'/'+rule.lstrip('/')+'.*')

        # Compile the rules and map it to the site
        # print 'Site rules =>',site_rules
        rules_c = [re.compile(i.lower()) for i in site_rules]
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
                freq = urlhelper.fetch_url(url)
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
        site = urlhelper.get_website(url)

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
    # Pass in init
    r = Robocop('http://www.askoy.kommune.no')
    ## # Check if can fetch
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
    print 'All tests passed.'
