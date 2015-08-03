# -- coding: utf-8
"""
Basic JS processing and extraction.

"""

"""
Original Copyright.

jsparser - This module provides classes which perform
Javascript extraction from HTML and Javascript parsing to
process DOM objects.

The module consists of two classes. HTMLJSParser
is an HTML Javascript extractor which can extract javascript
code present in HTML pages. JSParser builds upon HTMLJSParser
to provide a Javascript parser which can parse HTML pages
and process Javascript which performs DOM modifications.
Currently this class can process document.write* functions
and Javascript based redirection which changes the location
of a page.

Both classes are written trying to mimic the behaviour
of Firefox (2.0) as closely as possible.

This module is part of the HarvestMan program. For licensing
information see the file LICENSE.txt that is included in this
distribution.

Created Anand B Pillai <abpillai at gmail dot com> Aug 31 2007
Modified Anand B Pillai Oct 2 2007 Added JSParser class and renamed
                                   old JSParser to HTMLJSParser.

Modified Anand B Pillai  Jan 18 2008 Rewrote regular expressions in
                                     HTMLJSParser using pyparsing.

Mod      Anand B Pillai  Sep 8 2014  Added filters for catching errors
                                     in JS parsing like unexecuted
                                     JS code etc.

Copyright (C) 2007 Anand B Pillai.

"""

import sys, os
import re
import urllib2
import urlparse

from pyparsing import *
from jsdom import *

# Javscript string methods
__jssmethods__ = ('.charAt','.charCodeAt','.concat','.fromCharCode',
                  '.indexOf','.lastIndexOf','.localeCompare','.match',
                  '.replace','.search','.slice','.split','.substr',
                  '.substring','.toLocale','.toLowerCase','.toString',
                  '.toUpperCase','.trim','.valueOf')
                  

class HTMLJSParser(object):
   """ Javascript parser which extracts javascript statements
   embedded in HTML. The parser only performs extraction, and no
   Javascript tokenizing """

   script_content = Literal("<") + Literal("script") + ZeroOrMore(Word(alphas) + Literal("=") + Word(alphanums + "."+ "/"  + '"' + "'")) + Literal(">") + SkipTo(Literal("</") + Literal("script") + Literal(">"), True)

   comment_open = Literal("<!--") + SkipTo("\n", include=True)
   comment_close = Literal("//") + ZeroOrMore(Word(alphanums)) + Literal("-->")

   brace_open = Literal("{")
   brace_close = Literal("}")
   
   syntaxendre = re.compile(r';$')

   def __init__(self):
      self.comment_open.setParseAction(replaceWith(''))
      self.comment_close.setParseAction(replaceWith(''))
      self.brace_open.setParseAction(replaceWith(''))
      self.brace_close.setParseAction(replaceWith(''))            
      self.reset()
       
   def reset(self):
       self.rawdata = ''
       self.buffer = ''
       self.statements = []
       self.positions = []
       self.last_comment = ''

   def commentAction(self, string, loc, tokens):
      # print 'Strin =>',string
      # print 'LOC =>',loc,len(string), tokens
      if loc == 0:
         self.last_comment = ''.join(map(lambda x: x.strip(), string.split('\n', 1)))
         # print 'LAST COMMENT =>',self.last_comment
      else:
         if self.last_comment == string:
            self.last_comment = ""
            # This string is enclosed in quotes, so skip it
            # return ""
      
   def feed(self, data):

       self.rawdata = self.rawdata + data
       # Extract javascript content
       self.extract()
    
   # Internal - parse the HTML to extract Javascript
   def extract(self):

      rawdata = self.rawdata
      for match in self.script_content.scanString(rawdata):
         if not match: continue
         if len(match) != 3: continue
         if len(match[0])==0: continue
         if len(match[0][-1])==0: continue         
         statement = match[0][-1][0]
         # print 'Statement=>',statement
         self.statements.append(statement.strip())
         self.positions.append((match[-2], match[-1]))

      # print 'Length=>',len(self.statements)

      # If the JS is embedded in HTML comments <!--- js //-->
      # remove the comments. This logic takes care of trimming
      # any junk before/after the comments modeling the
      # behaviour of a browser (Firefox) as closely as possible.
      
      flag  = True
      # print 'STATEMENTS =>',self.statements
      # print '\tLENGTH =>',len(self.statements)
      for x in range(len(self.statements)):
         s = self.statements[x]
         # print 'Statement =>',s
         # Remove any braces
         # UPDATE: We need the { and } for detecting functions so commenting this.
         s = self.comment_open.transformString(s)
         s = self.comment_close.transformString(s)         

         # Clean up any syntax end chars
         s = self.syntaxendre.sub('', s).strip()

         # print 'S =>',s
         if s:self.statements[x] = s
      
class JSParserException(Exception):
   """ An exception class for JSParser """
   
   def __init__(self, error, context=None):
      self._error = error
      # Context: line number, js statement etc.
      self._context =context

   def __str__(self):
      return repr(self)

   def __repr__(self):
      return '@'.join((str(self._error), str(self._context)))

  
class JSParser(object):
   """ Parser for Javascript DOM. This class can be used to parse
   javascript which contains DOM binding statements. It returns
   a DOM object. Calling a repr() on this object will produce
   the modified DOM text """

   # TODO: Rewrite this using pyparsing

   # URL starting with http or https
   protocol_re = re.compile('https?')
   # Start signature of document.write* methods
   re1 = re.compile(r"(document\.write\s*\()|(document\.writeln\s*\()")
   
   re3 = re.compile(r'(?<![document\.write\s*|document\.writeln\s*])\(.*\)', re.MULTILINE)
   # End signature of document.write* methods
   re4 = re.compile(r'[\'\"]\s*\)|[\'\"]\s*\)', re.MULTILINE)
   # End of comments
   re_comment = re.compile(r'\/\/\s*--\>')
   
   # Pattern for contents inside document.write*(...) methods
   # This can be either a single string enclosed in quotes,
   # a set of strings concatenated using "+" or a set of
   # string arguments (individual or concatenated) separated
   # using commas. Text can be enclosed either in single or
   # double quotes.

   # Valid Examples...
   # 1. document.write("<H1>This is a heading</H1>\n");
   # 2. document.write("Hello World! ","Hello You! ","<p style='color:blue;'>Hello World!</p>");
   # 3. document.write("Hi, this is " + "<p>A paragraph</p>" + "and this is "  + "<p>Another one</p>");
   # 4. document.write("Hi, this is " + "<p>A paragraph</p>", "and this is "  + "<p>Another one</p>");

   # Pattern for content
   re5 = re.compile(r'(\".*\")|(\'.*\')', re.MULTILINE)
   re6 = re.compile(r'(?<=[\"\'\s])[\+\,]+')
   re7 = re.compile(r'(?<=[\"\'])(\s*[\+\,]+)')   
   re8 = re.compile(r'^[\'\"]|[\'\"]$')
   
   # JS redirect regular expressions
   # Form => window.location.replace("<url>") or window.location.assign("<url>")
   # or location.replace("<url>") or location.assign("<url>")
   jsredirect1 = re.compile(r'((window\.|this\.)?location\.(replace|assign))(\(.*\))', re.IGNORECASE)
   # Form => window.location.href="<url>" or location.href="<url>"
   jsredirect2 = re.compile(r'((window\.|this\.)location(\.href)?\s*\=\s*)(.*)', re.IGNORECASE)
   
   quotechars = re.compile(r'[\'\"]*')
   newlineplusre = re.compile(r'\n\s*\+')

   jsfunction_re = re.compile(r'function ([a-zA-Z0-9_\.]+)\s*\([^\)]*\)')

   # body onload locator
   bodyonload_re = re.compile(r'\<body\s+[^>]*onload="(.*?)"[^>]*\>', re.IGNORECASE)
   # document.ready locator
   documentready_re = re.compile(r'\$\(\s*document\s*\)\.ready\(\s*(.*?)\s*\)', re.IGNORECASE)
   # Anonymous jquery function of the form
   # $(...).func('elem', function(...)) ...
   anon_jquery_func = re.compile(r'\$\(\s*[\'\"]{1}[a-zA-Z0-9_\.]+[\'\"]{1}\)\.[a-zA-Z_]+\([\'\"]{1}[a-zA-Z0-9_]+[\'\"]{1}\s*,\s*function\s*\(\s*[a-zA-Z0-9_]*\s*\)')
   # Anonymous jquery function of the form
   # $(...).func(function(...)) ...   
   anon_jquery_func2 = re.compile(r'\$\(\s*[\'\"]{1}[a-zA-Z0-9_\.]+[\'\"]{1}\)\.[a-zA-Z_]+\(\s*function\s*\(\s*[a-zA-Z0-9_]*\s*\)')
   # jQuery functions of the form
   # jQuery(document).func(function(...))
   jquery_func = re.compile(r'jQuery\(\s*[\'\"]{1}[\#a-zA-Z0-9_\.]+[\'\"]{1}\)\.[a-zA-Z_]+\(\s*function\s*\(\s*[a-zA-Z0-9_]*\s*\)')
   # Javascript function taking arguments
   jsfunc_withargs = re.compile(r'function\([a-zA-Z0-9_]+[^\)]*\)')
   # Comments of the form <!-- content -->
   js_comments = re.compile(r'\<\!--.*\s*\/\/--\>', re.MULTILINE)
   # Punctuations we dont want in our redirect expressions
   unwanted_punctuations = ['|','||','+']
   
   # Maximum number of lines in a function for a redirection
   # NOTE - This is totally arbitrary and is not really a good workaround!
   MAXJSLINES = 50
    
   def __init__(self):
      self._nflag = False
      self.parser = HTMLJSParser()
      self.resetDOM()
      self.statements = []
      self.js = []
      # Location URLs
      self.locations = []
      self.location_changed = False
      self.dom_changed = False
      # Body onload handler
      self.onload_handler = None
      
      pass

   def resetDOM(self):
      self.page = None
      self.page = Window()
      self.page.document = Document()
      self.page.location = Location()
      self.location_changed = False
      self.dom_changed = False
      # Body onload handler
      self.onload_handler = None      
      
   def _find(self, data):
      # Search for document.write* statements and return the
      # match group if found. Also sets the internal newline
      # flag, depending on whether a document.write or
      # document.writeln was found.
      self._nflag = False
      m = self.re1.search(data)
      if m:
         grp = m.group()
         if grp.startswith('document.writeln'):
            self._nflag = True
         return m

   def parse_url(self, url):
      """ Parse data from the given URL """
      
      try:
         data = urllib2.urlopen(url).read()
         # print 'fetched data'
         return self.parse(data)
      except Exception, e:
         print e
         
         
   def parse(self, data):
      """ Parse HTML, extract javascript and process it """

      self.js = []
      self.statements = []
      self.resetDOM()

      self.parser.reset()
      # print 'Len of JS =>',len(self.parser.statements)
      
      self.page.document.content = data

      # Body onload or document.ready handlers
      body_handler = self.bodyonload_re.findall(data)
      # print 'BODY HANDLER =>',body_handler
      if len(body_handler):
         if body_handler[0].startswith('location.replace'):
            # E.g: http://www.valdepenas.es/ (issue #468)
            self.onload_handler = body_handler[0].strip()
         else:
            self.onload_handler = body_handler[0].replace('(','').replace(')','').strip()
         # print 'ONload handler =>',self.onload_handler
      else:
         jquery_handler = self.documentready_re.findall(data)
         if len(jquery_handler):
            self.onload_handler = jquery_handler[0].strip()

      # print 'Onload handler=>',self.onload_handler
      # Create a jsparser to extract content inside <script>...</script>
      # print 'Extracting js content...'
      self.parser.feed(data)
      self.js = self.parser.statements[:]
      
      # print 'Extracted js content.'
      if self.onload_handler != None and len(self.onload_handler):
         # print 'ONLOAD HANDLER APPENDED =>',self.onload_handler
         self.js.append(self.onload_handler)
         # print 'JS =>',self.js

      # print 'Found %d JS statements.' % len(self.js)
      
      # print 'Processing JS'
      for x in range(len(self.js)):
         statement = self.js[x]

         # First check for JS redirects and
         # then for JS document changes.
         jsredirect = self.processLocation(statement)
         if jsredirect:
            # No need to process further since we are redirecting
            # the location
            break
         else:
            # Further process the URL for document changes
            try:
               position = self.parser.positions[x]
            except IndexError:
               continue
            
            rawdata = statement.strip()
            self._feed(rawdata)
         
            if len(self.statements):
               self.processDocument(position)

      # Set flags for DOM/Location change
      self.location_changed = self.page.location.hrefchanged
      self.dom_changed = self.page.document.contentchanged

      # print 'Processed JS.'
      
   def processDocument(self, position):
      """ Process DOM document javascript """

      # The argument 'position' is a two tuple
      # containing the start and end positions of
      # the javascript tags inside the document.

      dom = self.page.document
      start, end = position
      
      # Reset positions on DOM content to empty string
      dom.chomp(start, end)
      
      for text, newline in self.statements:
         if newline:
            dom.writeln(text)
         else:
            dom.write(text)

      # Re-create content
      dom.construct()

   # Internal - validate URL strings for Javascript
   def validate_url(self, urlstring):
      """ Perform validation of URL strings """

      urlstring = urlstring.strip()

      # Validate the URL - This follows Firefox behaviour
      # In firefox, the URL might be or might not be enclosed
      # in quotes. However if it is enclosed in quotes the quote
      # character at start and begin should match. For example
      # 'http://www.struer.dk/webtop/site.asp?site=5',
      # "http://www.struer.dk/webtop/site.asp?site=5" and
      # http://www.struer.dk/webtop/site.asp?site=5 are valid, but
      # "http://www.struer.dk/webtop/site.asp?site=5' and
      # 'http://www.struer.dk/webtop/site.asp?site=5" are not.
      if urlstring.startswith("'") or urlstring.startswith('"'):
         if urlstring[0] != urlstring[-1]:
            # print 'Invalid URL => ',urlstring
            # Invalid URL
            return False

      # Javascript variable/method check - a number of times the parser
      # wrongly thinks pieces of JS code as URLs. This is a check for that.
      # Usual suspects.
      # 1. url
      # 2. anything starting with document.
      # 3. value
      # 4. Unresolved JS method calls on string as part of URL

      # print 'URL STRING =>',urlstring
      if urlstring in ('url','value', 'loc') or any(x in urlstring for x in __jssmethods__):
         return False

      # Anything that looks like a method call or attribute access.
      if any(map(urlstring.startswith, ('.','this.','document.', 'window.'))):
         return False
      
      # Additional validation - Issue 427 for URL http://www.qnb.com.qa/cs/Satellite/QNBGlobal/en/enGlobalHome
      # Parse the URL - at least one of the elements of the parsed
      # object should be non-empty.
      p = urlparse.urlparse(urlstring)

      if all(getattr(p, field) == '' for field in p._fields):
         return False

      # Further validation - no part of the URL should be a JS variable such as $x
      if any(map(lambda x: x.strip()[0]=='$', urlstring.split())):
         return False

      if not self.protocol_re.search(urlstring) and (not '/' in urlstring) and (not '.' in urlstring):
         print '## Skipping',urlstring
         return False
             
      return True

   def make_valid_url(self, urlstring):
      """ Create a valid URL string from the passed urlstring """
      
      # Strip off any leading/trailing quote chars
      urlstring = self.quotechars.sub('',urlstring)
      return urlstring.strip()
     
   def processLocation(self, statement):
      """ Process any changes in document location """

      location_changed = False
      # Reset
      self.locations = []
      
      # Issue #427 - Size of the JS statement/function in number of lines
      # We skip anything more than 10 lines since it typically means it
      # is a long function which is not a simple JS URL replacement.

      # If a statement starts with an if ... then this means conditional
      # code so skip straight away.
      # print statement
      statement = statement.strip()
      # print 'Statement =>', statement
      if statement.startswith('if '):
         return False

      # Don't do try... catch blocks
      # if statement.startswith('try '):
      #   return False
      
      if (self.anon_jquery_func.search(statement) or
          self.anon_jquery_func2.search(statement) or
          self.jquery_func.search(statement) or
          self.js_comments.search(statement)):
         return False
      
      js_lines = statement.split('\n')
      if len(js_lines)>self.MAXJSLINES:
         # print 'debug: skipping JS statement for redirection since number of lines',len(js_lines),'>',self.MAXJSLINES
         return False 

      start_f, comment_open, fname = False, False, ''
      braces = 0
      functions = {}

      prev_line = ''
      
      for line in js_lines:
         line = line.strip()
         # Skip empty lines
         if len(line)==0: continue

         # If commented out code, then skip...
         # Single-line comment
         if line.startswith('//'):
            # print 'Skipping',line,'...'            
            continue

         # Multiline comment
         if line.startswith('/*'):
            comment_open = True

         if comment_open:
            if line.endswith('*/'):
               comment_open = False
            else:
               # Skip the line
               # print 'Skipping',line,'...'
               continue

         # If previous line is an if condition then skip
         if prev_line.startswith('if ') or prev_line.startswith('else if'):
            # print 'Skipping',line,'since previous line is an if condition ...'
            # Reset previous line
            prev_line = line
            continue

         # If previous line is a function taking arguments then less chance it supports
         # an un-conditional redirect...
         if self.jsfunc_withargs.search(prev_line):
            # print 'Skipping since previous line is function taking non-empty argumeents'
            prev_line = line
            continue
         
         # Check if start of function
         fmatch = self.jsfunction_re.findall(line)
         if len(fmatch):
            # Function name
            fname = fmatch[0]
            # Indicates start of function
            start_f = True
            # print 'Start of function => {',fname,'}.'
               
         if start_f:
            # Check for braces
            if '{' in line:
               # print '{ =>',fname
               functions[fname] = False
               braces += 1
            if '}' in line:
               # print '} =>',fname               
               braces -= 1

            if fname in functions and braces==0:
               # End of function
               start_f = False
               # print 'End of function => {',fname,'}.'               

            # If this is not the body onLoad function skip it ...
            if self.onload_handler:
               if (fname != self.onload_handler):
                  # print 'Skipping function => {',fname,'} since it is not onload handler.'
                  continue
            else:
               # Skip functions anyway if we don't find onload handler
               # print 'Onload handler not found, skipping function => {',fname,'} anyway.'
               continue
            

         # If line contains some punctuations like || then skip the line.
         if any(map(lambda x: x in line, self.unwanted_punctuations)):
            # print 'Skipping line',line
            continue
         
         # print 'Expression=>',line

         # The URL has to have either http in it or end with something '.html'
         # or have a slash in it etc.
         
         m1 = self.jsredirect1.search(line)
         if m1:
            tokens = self.jsredirect1.findall(line)
            # print 'TOKENS=>',tokens
            if tokens:
                urltoken = tokens[0][-1]
                # Strip of trailing and leading parents and also any semicolons
                url = urltoken.replace('(','').replace(')','').replace(';','').replace('}','').replace('{','').strip()
                # Validate URL
                if self.validate_url(url):
                   url = self.make_valid_url(url)
                   location_changed = True
                   print 'Replacing location with',url
                   self.locations.append(url)
                   self.page.location.replace(url)
                else:
                   print 'info: URL =>',url,'<= is not valid.'
         else:
            m2 = self.jsredirect2.search(line)
            if m2:
               tokens = self.jsredirect2.findall(line)
               # print 'TOKENS=>',tokens
               
               urltoken = tokens[0][-1]
               # Strip of trailing and leading parents
               url = urltoken.replace('(','').replace(')','').replace(';','').replace('}','').replace('{','').strip()
                
               if tokens and self.validate_url(url):
                  url = self.make_valid_url(url)
                  location_changed = True
                  self.page.location.replace(url)
                  print 'Replacing location with',url
                  self.locations.append(url)                  
                  location_changed = True
               else:
                   print 'info: URL =>',url,'<= is not valid.'                  

         # Save line
         prev_line = line

      # If > 1 location URLs, chose the "best"
      if len(self.locations)>1:
         url = self.choose_best_location()
         if len(url):
            # print 'Best location=>',url
            self.page.location.replace(url)
         else:
            # No location change
            return False
            
      return location_changed

   def choose_best_location(self):
      """ If there are > 1 JS location URLs, choose among them according
      to some heuristics """

      # Prefer absolute URLs over relative ones i.e one with netloc
      locations2 = [loc for loc in self.locations if urlparse.urlparse(loc).netloc != '']
      # If nothing selected, skip this test
      if len(locations2)==0:
         locations2 = self.locations
         # means all relative URLs - so drop the ones with single path fragment
         locations2 = [loc for loc in locations2 if len(urlparse.urlparse(loc).path.split('/')) == 1]
      
      # Now select the ones with no query or fragments
      locations3 = [loc for loc in locations2 if urlparse.urlparse(loc).query == '']
      # If nothing selected, skip this test
      if len(locations3)==0:
         locations3 = locations2

      # Choose the shortest
      locations4 = sorted(locations3, key=len)
      if len(locations4):
         # Return 1st
         return locations4[0]

      return ''

   def _feed(self, data):
      """ Internal method to feed data to process DOM document """
      
      self.statements = []
      self.rawdata = data
      self.goahead()
      self.process()
      
   def tryQuoteException(self, line):
      """ Check line for mismatching quotes """
      
      ret = 0
      # Check line for mismatching quotes
      if line[0] in ("'",'"') and line[-1] in ("'",'"'):
         ret = 1
         if line[0] != line[-1]:
            raise JSParserException("Mismatching quote characters", line)

      return ret
   
   def process(self):
      """ Process DOM document related javascript """

      # Process extracted statements
      statements2 = []
      for s, nflag in self.statements:

         m = self.re5.match(s)
         if m:
            # Well behaved string
            if self.re6.search(s):
               m = self.re7.search(s)
               newline = self.newlineplusre.match(m.groups(1)[0])
               items = self.re6.split(s)
               
               # See if any entry in the list has mismatching quotes, then
               # raise an error...
               for item in items:
                  # print 'Item=>',item
                  self.tryQuoteException(item)
                  
               # Remove any trailing or beginning quotes from the items
               items = [self.re8.sub('',item.strip()) for item in items]
               # Replace any \" with "
               items = [item.replace("\\\"", '"') for item in items]

               # If the javascript consists of many lines with a +
               # connecting them, there is a very good chance that it
               # breaks spaces across multiple lines. In such case we
               # need to join the pieces with at least a space.
               if newline:
                  s = ' '.join(items)
               else:
                  # If it does not consist of newline and a +, we don't
                  # need any spaces between the pieces.
                  s = ''.join(items)                  

            # Remove any trailing or beginning quotes from the statement
            s = self.re8.sub('', s)
            statements2.append((s, nflag))
         else:
            # Ill-behaved string, has some strange char either beginning
            # or end of line which was passed up to here.
            # print 'no match',s
            # Split and check for mismatched quotes
            if self.re6.search(s):
               items = self.re6.split(s)
               # See if any entry in the list has mismatching quotes, then
               # raise an error...
               for item in items:
                  self.tryQuoteException(item)
                  
            else:
               # Ignore it
               pass
            
      self.statements = statements2[:]
      pass
   
   def goahead(self):

      rawdata = self.rawdata
      self._nflag = False
      
      # Scans the document for document.write* statements
      # At the end of parsing, an internal DOM object
      # will contain the modified DOM if any.

      while rawdata:
         m = self._find(rawdata)
         if m:
            # Get start of contents
            start = m.end()
            rawdata = rawdata[start:]
            # Find the next occurence of a ')'
            # First exclude any occurences of pairs of parens
            # in the content
            contentdata, pos = rawdata, 0
            m1 = self.re3.search(contentdata)
            while m1:
               contentdata = contentdata[m1.end():]
               pos = m1.end()
               # print 'Pos=>',pos
               # print contentdata
               m1 = self.re3.search(contentdata)
               
            m2 = self.re4.search(rawdata, pos)
            if not m2:
               # If this is end of comment, then fine, otherwise raise error
               # as it has to be an end paren then.
               if not self.re_comment.search(rawdata, pos):
                  raise JSParserException('Missing end paren!')
            else:
               start = m2.start()
               statement = rawdata[:start+1].strip()
               # print 'Statement=>',statement
               # If statement contains a document.write*, then it is a
               # botched up javascript, so raise error
               if self.re1.search(statement):
                  raise JSParserException('Invalid javascript', statement)
               
               # Look for errors like mismatching start and end quote chars
               if self.tryQuoteException(statement) == 1:
                  pass
               elif statement[0] in ('+','-') and statement[-1] in ("'", '"'):
                  # Firefox seems to accept this case
                  print 'warning: garbage char "%s" in beginning of statement!' % statement[0]
               #elif statement[0] not in ('"', "'") and statement[-1] not in ("'", '"'):
               #   # Sometimes the JS might be a function in which case we cannot
               #   # expect it to start with single or double quotes. 
               #   pass
               #else:
               #   raise JSParserException("Garbage in beginning/end of statement!", statement)
                  
               # Add statement to list
               self.statements.append((statement, self._nflag))
               rawdata = rawdata[m2.end():]
         else:
            # No document.write* content found
            # print 'no content'
            break

   def getDocument(self):
      """ Return the DOM document object, this can be used to get
      the modified page if any """

      return self.page.document

   def getLocation(self):
      """ Return the DOM Location object, this can be used to
      get modified URL if any """

      return self.page.location

   def getStatements(self):
      """ Return the javascript statements in a list """

      return [item for item in self.parser.statements if item.strip()]
   
def localtests():
    print 'Doing local tests...'
    
    P = JSParser()
    P.parse(open('samples/bportugal.html').read())
    assert(repr(P.getDocument())==open('samples/bportugal_dom.html').read())
    assert(P.dom_changed==True)
    assert(P.location_changed==False)
    
    P.parse(open('samples/jstest.html').read())
    assert(repr(P.getDocument())==open('samples/jstest_dom.html').read())
    assert(P.dom_changed==True)
    assert(P.location_changed==False)

    P.parse(open('samples/jsnodom.html').read())
    assert(repr(P.getDocument())==open('samples/jsnodom.html').read())
    assert(P.dom_changed==False)
    assert(P.location_changed==False)    

    P.parse(open('samples/jstest2.html').read())
    assert(repr(P.getDocument())==open('samples/jstest2_dom.html').read())
    assert(P.dom_changed==True)
    assert(P.location_changed==False)    

    P.parse(open('samples/jstest3.html').read())
    assert(repr(P.getDocument())==open('samples/jstest3_dom.html').read())
    assert(P.dom_changed==True)
    assert(P.location_changed==False)

    P.parse(open('samples/jsredirect.html').read())
    assert(repr(P.getDocument())==open('samples/jsredirect.html').read())
    assert(P.dom_changed==False)
    assert(P.location_changed==True)
    assert(P.getLocation().href=="http://www.struer.dk/webtop/site.asp?site=5")

    P.parse(open('samples/jsredirect4.html').read())
    assert(repr(P.getDocument())==open('samples/jsredirect4.html').read())
    assert(P.dom_changed==False)
    assert(P.location_changed==True)
    assert(P.getLocation().href=="http://www.szszm.hu/szigetszentmiklos.hu")
    
    P.parse(open('samples/jsredirect5.html').read())
    assert(repr(P.getDocument())==open('samples/jsredirect5.html').read())
    assert(P.dom_changed==False)
    assert(P.location_changed==True)
    assert(P.getLocation().href=="sopron/main.php")    

    P.parse(open('samples/www_thegroup_com_qa.html').read())
    assert(P.location_changed==False)

    P.parse(open('samples/qnb_redirect.html').read())
    assert(P.location_changed==False)

    P.parse(open('samples/baladiya_gov_qa.html').read())
    assert(P.location_changed==True)
    assert(P.getLocation().href == 'http://www.baladiya.gov.qa/cui/index.dox')

    P.parse(open('samples/alrayyan_tv.html').read())
    assert(P.location_changed==False)

    P.parse(open('samples/raya.html').read())
    assert(P.location_changed==False)    
    print P.getLocation().href

    P.parse(open('samples/qmediame.html').read())
    assert(P.location_changed==True)        
    
    P.parse(open('samples/www.qdb.qa.html').read())
    assert(P.location_changed==True)            
    print P.location_changed
    print P.getLocation().href

    P.parse(open('samples/ralingen_kommune.html').read())
    assert(P.location_changed==False)                
    # print P.getLocation().href

    P.parse(open('samples/hoyanger_kommune.html').read())
    assert(P.location_changed==False)                
    # print P.getLocation().href

    P.parse(open('samples/sor-varanger_kommune.html').read())
    assert(P.location_changed==False)                
    # print P.getLocation().href 

    P.parse(open('samples/tysver_kommune.html').read())
    assert(P.location_changed==False)                    
    # print P.getLocation().href

    P.parse(open('samples/lehavre_fr.html').read())
    assert(P.location_changed==False)                        
    # print P.getLocation().href

    P.parse(open('samples/policija_lt.html').read())
    assert(P.location_changed==False)                        
    print P.getLocation().href        

    P.parse(open('samples/guichet_public_lu.html').read())
    assert(P.location_changed==False)

    P.parse(open('samples/welfare_ie.html').read())
    assert(P.location_changed==False)

    P.parse(open('samples/metalib.html').read())
    assert(P.location_changed==False)

    P.parse(open('samples/uhu_es.html').read())
    assert(P.location_changed==False)

    P.parse(open('samples/udbudsavisen_dk.html').read())
    assert(P.location_changed==False)

    P.parse(open('samples/statistics_gr.html').read())
    assert(P.location_changed==False)

    P.parse(open('samples/yeditepe_edu_tr.html').read())
    assert(P.location_changed==False)

    P.parse(open('samples/valdepenas_es.html').read())
    assert(P.location_changed== True)
    print P.getLocation().href
    
    P.parse(open('samples/rtu_lv.html').read())
    assert(P.location_changed==False)                    
    
    print 'All local tests passed.'

def webtests():
    print 'Starting web tests...'
    P = JSParser()

    urls = [("http://www.skien.kommune.no/", 7), ("http://www.bayern.de/", 3),
            ("http://www.agsbs.ch/", 0), ("http://www.froideville.ch/", 1)]

    for url, number in urls:
       print 'Parsing URL %s...' % url
       P.parse_url(url)
       print 'Found %d statements.' % len(P.getStatements())
       assert(number==len(P.getStatements()))
       
def experiments():
   P = JSParser()
   P.parse(open('samples/test.html').read())
   
if __name__ == "__main__":
   localtests()
   # webtests()
   # experiments()
   
   
    


