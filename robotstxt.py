# -- coding: utf-8

import urllib2

___author__ = "Anand B Pillai"
__maintainer__ = "Anand B Pillai"
__version__ = "0.1"
__lastmodified__ = "2013-03-25 17:53:48 IST"


# TODO: This should be done better...
# http://nikitathespider.com/python/rerp/ Does not work...

class robotstxt:
    
    def __init__(self,site):
        robotsfile = 'http://'+site+'/robots.txt'
        try:
            content = urllib2.urlopen(robotsfile).read().split('\n')
        except:
            content = []
        self.sitemap = ''
        self.rules = []

        useragent = '*'
        disallowallow = 'disallow'

        self.crawldelay = -1

        for line in content:
            if line.lower().strip().startswith('sitemap:'):
                self.sitemap = ':'.join(line.split(':')[1:]).strip()
                continue
            if line.lower().strip().startswith('user-agent:'):
                useragent = ':'.join(line.split(':')[1:]).strip()
                print 'user-agent:',useragent
                continue
            if line.lower().strip().startswith('#'):
                continue
            if 'disallow' in line.lower():
                disallowallow = 'disallow'
            elif 'allow' in line.lower():
                disallowallow = 'allow'
            if (line.lower().startswith('crawldelay:') or line.lower().startswith('crawl-delay:')) and useragent in ('*','eGovMon'):
                self.crawldelay = int(':'.join(line.split(':')[1:]).strip())
                continue
            if disallowallow == 'disallow' and useragent in ('*','eGovMon'):
                #print useragent,disallowallow,line#,self.rules
                rules = ':'.join(line.split(':')[1:]).strip().split()
                for rule in rules:
                    if rule.strip():
                        if 'http' in rule:
                            self.rules.append(rule + '.*')
                        else:
                            self.rules.append('https?://'+site+'/'+rule.lstrip('/')+'.*')

              
if __name__ == '__main__':
    r = robotstxt('www.askoy.kommune.no')
    print 'Rules:'
    for rule in r.rules:
        print rule
