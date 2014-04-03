# -- coding: utf-8
""" Robots.txt parsing and META robots tags checking """

import urlhelper
import urlparse
import re

___author__ = "Anand B Pillai"
__maintainer__ = "Anand B Pillai"
__version__ = "0.1"

# TODO: This should be done better...
# http://nikitathespider.com/python/rerp/ Does not work...

class Robocop(object):
    """ Robots.txt parser for websites """

    meta_re = re.compile(r'\<meta\s+name=\"robots\"\s+content=\"(.*?)\"\s*\>', re.IGNORECASE)
    
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
            freq = urlhelper.get_url(robots_url)
            content = freq.content.split('\n')
            self.parse_robotstxt(content, site_nos)
        except:
            content = []

    def parse_robotstxt(self, content, site):
        """ Parse the robots.txt content """
        
        sitemap = ''
        site_rules = []

        useragent = '*'
        disallowallow = 'disallow'

        self.crawldelay = -1

        for line in content:
            if line.lower().strip().startswith('sitemap:'):
                # CHECKME: So ? This is not being used further or what...
                sitemap = ':'.join(line.split(':')[1:]).strip()
                continue
            if line.lower().strip().startswith('user-agent:'):
                useragent = ':'.join(line.split(':')[1:]).strip()
                # print 'user-agent:',useragent
                continue
            if line.lower().strip().startswith('#'):
                continue
            if 'disallow' in line.lower():
                disallowallow = 'disallow'
            elif 'allow' in line.lower():
                disallowallow = 'allow'
            if (line.lower().startswith('crawldelay:') or line.lower().startswith('crawl-delay:')) and useragent in ('*',self.ua):
                self.crawldelay = int(':'.join(line.split(':')[1:]).strip())
                continue
            
            if disallowallow == 'disallow' and useragent in ('*', self.ua):
                #print useragent,disallowallow,line#,rules
                rules = ':'.join(line.split(':')[1:]).strip().split()
                for rule in rules:
                    if rule.strip():
                        if 'http' in rule:
                            site_rules.append(rule + '.*')
                        else:
                            site_rules.append('https?://'+site+'/'+rule.lstrip('/')+'.*')

        # Compile the rules and map it to the site
        # print 'Site rules =>',site_rules
        rules_c = [re.compile(i.lower()) for i in site_rules]
        self.rules[site] = rules_c

    def check_meta(self, url, content=None):
        """ Check META robots tags and return a 2-tuple of
        booleans (can_index, can_follow) for the URL """

        if content==None:
            # Need to fetch the content
            try:
                freq = urlhelper.get_url(url)
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
                rules = sorted([x.strip() for x in meta_match[0].lower().split(',')], reverse=True)
                # print 'Rules=>', (rules[0] == 'index', rules[1] == 'follow')
                return (rules[0] == 'index', rules[1] == 'follow')
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
        site_rules = self.rules.get(site)

        # Prefix scheme in front of URL
        url = urlhelper.get_full_url(url)

        if site_rules != None:
            # Check against the rules
            if any(rule.match(url) for rule in site_rules):
                return False

        elif not meta:
            # Nothing can be done as no rules configured, so returns default
            # value of allow (True)
            # print 'No rules configured for site',site,',returning default value'
            return True
        
        # Check meta if asked
        if meta: 
            return self.check_meta(url, content)[0]

        return True
    
                  
if __name__ == '__main__':
    r = Robocop('www.askoy.kommune.no')
    # Check if can fetch
    print r.can_fetch('www.askoy.kommune.no/cache/test')
    print r.can_fetch('www.askoy.kommune.no/logs/log1.log')
    print r.can_fetch('www.askoy.kommune.no/pictres/1.jpg')     

    r.parse_site('http://www.metatags.info/')
    print r.can_fetch('http://www.metatags.info/meta_name_robots', meta=True)
    print r.can_fetch('http://www.metatags.info/cpanel/testing')    
    print r.can_fetch('http://www.robotstxt.org/meta.html', meta=True)