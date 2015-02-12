# -- coding: utf-8

""" Classes and functions for feature/template extraction by the crawler """

import re
import utils

# Social-share footer - http://dev.tingtun.no/search-engine/search-engine-backend/issues/380
social_footer_re = re.compile(r'<div\s+id\=\"social\-share\">(.*?)<\/div>', re.IGNORECASE|re.DOTALL)
# Main & footer regex using divs
maincontent_re = re.compile(r'\<div[^-][^>]*id\=(\"maincontent|\"maincontents|\"main\-content|\"mainbody|\"main\-body|\"mainarea|\"main\-area)[^-][^>]*\>', re.IGNORECASE|re.DOTALL|re.UNICODE)
maincontent2_re = re.compile(r'\<div[^-][^>]*role\=(\"main|\"maincontent|\"main\-content|\"mainbody|\"main\-body|\"mainarea|\"main\-area)[^-][^>]*\>', re.IGNORECASE|re.DOTALL|re.UNICODE)
footer_re = re.compile(r'\<div[^-][^>]*id\=(\"footercontent|\"footer\-content|\"footerarea|\"footer\-area|\"[a-zA-Z0-9_\-]+footer\")', re.IGNORECASE|re.DOTALL|re.UNICODE)
footer_re2 = re.compile(r'\<footer\s*[^>]*\>', re.IGNORECASE)

class SmartWordTextProcessor(object):
    """ Class for identifying common prefix and suffix of pages in a site
    by sampling. Returns common prefix (menu) and suffix (footer) which can
    be removed in the word text for the site.

    Helpful in identifying and removing menu and footer content from
    websites while crawling """
    
    __metaclass__ = utils.SingletonMeta

    def __init__(self):
        self.texts = []
        self.count = 0
        self.space_end = re.compile(r'\s+$')
        self.common_prefixes = {}
        self.common_suffixes = {}
        
    def addText(self, text):
        """ Add texts for processing """
        
        self.texts.append(text)
        # Process every 5th time
        self.count += 1
        if self.count % 5 == 0:
            cprefix, suffix = self.process()
            if cprefix:
                self.common_prefixes[self.count] = cprefix
            if suffix:
                self.common_suffixes[self.count] = suffix               
                
            # Reset
            self.texts = []

    def _commonPrefix(self, a, b):
        """ Return common prefix of two strings a & b """

        try:
            common = a[:[x[0]==x[1] for x in zip(a,b)].index(0)]
        except ValueError:
            # Means both strings are same or one string is fully
            # contained in the other.
            common = (a if len(a)<len(b) else b)
        
        # Return till last space character
        try:
            if self.space_end.search(common):
                return common
            else:
                return common[:common.rindex(' ')].strip()
        except ValueError:
            return common

    def _commonSuffix(self, a, b):
        """ Return common suffix of two strings a & b """

        # Reverse the text, get the common prefix and
        # reverse text again.
        suffix = self._commonPrefix(a[-1::-1], b[-1::-1])
        # Reverse it
        return suffix[-1::-1]
        
    def process(self):
        """ Process text looking for common content """

        # Shuffle and pick top half
        prefixes, suffixes = [], []
        
        for x in range(len(self.texts)):
            random.shuffle(self.texts)
            # Pick two texts
            text1, text2 = self.texts[0], self.texts[1]
            prefix = self._commonPrefix(text1, text2)

            if prefix:
                prefixes.append(prefix)

            suffix = self._commonSuffix(text1, text2)
            if suffix:
                suffixes.append(suffix)             

        if len(prefixes)==0:
            return ''
        
        prefix_counts, suffix_counts = {}, {}

        for prefix in set(prefixes):
            prefix_counts[prefix] = 100.0*prefixes.count(prefix)/len(prefixes)

        for suffix in set(suffixes):
            suffix_counts[suffix] = 100.0*suffixes.count(suffix)/len(suffixes)
            
        prefix_sel = sorted(prefix_counts, key=prefix_counts.get, reverse=True)[0]
        suffix_sel = sorted(suffix_counts, key=suffix_counts.get, reverse=True)[0]        

        return (prefix_sel, suffix_sel)

    def getCommonPrefix(self):
        """ Return most commonly occuring common prefix """
        
        # Do this if at least two tests have been performed
        if self.count >= 5:
            prefixes = self.common_prefixes.values()
            unique = set(prefixes)

            counts = {}

            for prefix in unique:
                counts[prefix] = 100.0*prefixes.count(prefix)/len(prefixes)
            
            # return sorted(counts, key=counts.get, reverse=True)[0]
            allprefixes = list(set(counts.keys()))
            # Sort by decreasing length
            return sorted(allprefixes, key=len, reverse=True)
        else:
            return None

    def getCommonSuffix(self):
        """ Return most commonly occuring common suffix """
        
        # Do this if at least two tests have been performed
        if self.count >= 5:
            suffixes = self.common_suffixes.values()
            unique = set(suffixes)

            counts = {}

            for suffix in unique:
                counts[suffix] = 100.0*suffixes.count(suffix)/len(suffixes)
            
            # return sorted(counts, key=counts.get, reverse=True)[0]
            allsuffixes = list(set(counts.keys()))
            # Sort by decreasing length
            return sorted(allsuffixes, key=len, reverse=True)
        else:
            return None     
    
def get_footer(content):
    """ Return footer content by using common footer divs.
    If nothing matches, returns None """

    footer_match = footer_re.search(content)
    if footer_match != None:
        # Strip off footer text if present
        content = content[footer_match.start():].strip()
        return content

    footer_match = footer_re2.search(content)
    if footer_match != None:
        # Strip off footer text if present
        content = content[footer_match.start():].strip()
        return content  

def get_header(content):
    """ Return header/menu content by using common
    main content divs.

    If nothing matches, returns None """

    matched = False

    content_match = maincontent_re.search(content)
    if content_match != None:
        # Everything till the main-content
        content = content[:content_match.start()].strip()     
        return content
    
    content_match = maincontent2_re.search(content)
    if content_match != None:
        # Everything till the main-content
        content = content[:content_match.start()].strip()     
        return content
    



    
   
