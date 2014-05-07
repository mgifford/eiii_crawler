import inspect
import requests
import itertools
import os
import json
import datetime

from contextlib import contextmanager
from types import StringTypes
# Global logger object
g_logger = None

class CaselessDict(dict):
    """ Dictionary with case-insensitive keys """
    
    def __init__(self, mapping=None):
        if mapping:
            if type(mapping) is dict:
                for k,v in d.items():
                    self.__setitem__(k, v)
            elif type(mapping) in (list, tuple):
                d = dict(mapping)
                for k,v in d.items():
                    self.__setitem__(k, v)
                    
        # super(CaselessDict, self).__init__(d)
        
    def __setitem__(self, name, value):

        if type(name) in StringTypes:
            super(CaselessDict, self).__setitem__(name.lower(), value)
        else:
            super(CaselessDict, self).__setitem__(name, value)

    def __getitem__(self, name):
        if type(name) in StringTypes:
            return super(CaselessDict, self).__getitem__(name.lower())
        else:
            return super(CaselessDict, self).__getitem__(name)

    def __copy__(self):
        pass
    
class SingletonMeta(type):
    """ A type for Singleton classes """    

    def __init__(cls, *args):
        type.__init__(cls, *args)
        cls.instance = None

    def __call__(cls, *args, **kwargs):
        if not cls.instance:
            cls.instance = type.__call__(cls, *args, **kwargs)
        return cls.instance

    def getInstance(cls, *args, **kwargs):
        """ Return an instance """
        return cls(*args, **kwargs)

class MyEncoder(json.JSONEncoder):
 
    def default(self, obj):
        if any(isinstance(obj, x) for x in (datetime.datetime, datetime.date, datetime.timedelta)):
            return str(obj)

        # print 'OBJ=>',obj
        return json.JSONEncoder.default(self, obj)
    
def getPreviousFrame(n=2):
    """ Get the stack frame at levels 'n' above
    the current stack frame """

    current = inspect.currentframe()
    frame = eval('current ' + '.f_back'*n)
    return inspect.getframeinfo(frame)

def getPreviousFrameCaller(n=2):
    """ Get the stack frame caller info (modulename, function)
    at levels 'n' above the current stack frame """

    current = inspect.currentframe()
    frame = eval('current ' + '.f_back'*n)
    caller = inspect.getframeinfo(frame)

    return (caller.filename, caller.function)

def setGlobalLogger(logger):
    """ Set the global logger object """

    global g_logger
    g_logger = logger

def logMessage(msg, *args):
    """ Log message with variable arguments """

    try:
        msg = ' '.join([str(msg)] + map(lambda x: str(x), args))
        print msg
    except:
        print >> sys.stdout, msg, args

# START - Logging functions
def info(msg, *args):
    """ Log at info level """

    if g_logger:
        sourcename, function = '',''
        # Check if debugging is enabled on the logger
        if g_logger.getDebug():
            sourcename, function = getPreviousFrameCaller()
            
        return g_logger.info(msg, *args, sourcename=sourcename, function=function)
    else:
        logMessage(msg, *args)

def warning(msg, *args):
    """ Log at warning level """

    if g_logger:
        sourcename, function = '',''
        # Check if debugging is enabled on the logger
        if g_logger.getDebug():
            # Get calling frame
            sourcename, function = getPreviousFrameCaller()         
            
        return g_logger.warning(msg, *args, sourcename=sourcename, function=function)
    else:
        logMessage(msg, *args)

def error(msg, *args):
    """ Log at error level """

    if g_logger:
        sourcename, function = '',''
        # Check if debugging is enabled on the logger
        if g_logger.getDebug():
            sourcename, function = getPreviousFrameCaller()
            
        return g_logger.error(msg, *args,sourcename=sourcename, function=function)
    else:
        logMessage(msg, *args)

def critical(msg, *args):
    """ Log at critical level """

    if g_logger:
        sourcename, function = '',''
        # Check if debugging is enabled on the logger
        if g_logger.getDebug():
            sourcename, function = getPreviousFrameCaller()
            
        return g_logger.critical(msg, *args, sourcename=sourcename, function=function)
    else:
        logMessage(msg, *args)

def debug(msg, *args):
    """ Log at debug level """

    if g_logger:
        sourcename, function = '',''
        # Check if debugging is enabled on the logger
        if g_logger.getDebug():
            sourcename, function = getPreviousFrameCaller()
            
        return g_logger.debug(msg, *args, sourcename=sourcename, function=function)
    else:
        logMessage(msg, *args)

def logsimple(msg, *args):
    """ Log a string with no formatting """

    if g_logger:
        return g_logger.logsimple(msg, *args)
    else:
        logMessage(msg, *args)
        
# END - Logging functions        
def cleanurl(url):
    if '#' in url:
        url = url[:url.index('#')]
    return url.rstrip('/&')


@contextmanager
def ignored(*exceptions):
    """ Ignore specific chain of exceptions

    >>> import os
    >>> with ignored(OSError):
    ...     os.remove('notfound')
    ... 
    >>> 
    """

    try:
        yield
    except exceptions:
        pass

# Ignore exceptions
class ignore(object):
    """ Context manager class for ignoring exceptions
    generated in the dependent block.

    >>> x='python'
    >>> try:
    ...     x.index('.')
    ... except ValueError, e:
    ...     print e
    ... 
    substring not found
    >>> with ignore():
    ...     x.index('.')
    ... 
    """
    
    def __enter__(self):
        pass
    
    def __exit__(self, type, value, traceback):
        # According to PEP 343 returning True from
        # __exit__ causes the context manager to ignore
        # exceptions generated in the BLOCK.
        return True

def create_cache_structure(root='.'):
    """ Create folder structure for writing cache """

    print "Creating cache directories..."
    # All combinations
    hexchars = 'abcdef0123456789'

    for i in itertools.product(hexchars, hexchars):
        folder = os.path.join(root, ''.join(i))
        if not os.path.exists(folder):
            os.makedirs(folder)

    print "done."

def convert_config(crawler_rules):
    """ Convert config rules from Checker's format
    to the format understood by EIII crawler.

    # NOTE: This doctest fails. This is just for reference.
    
    >>> cfg = {'max-pages': [(['text/html', 'application/xhtml+xml', 'application/xml'], 6000)],
    ...        'scoping-rules': [('+', '^https?://utt\\.tingtun\\.no')], 'min-crawl-delay': 2,
    ...        'size-limits': [(['text/html', 'application/xhtml+xml', 'application/xml'], 500)],
    ...        'seeds': ['http://www.tingtun.no', 'http://utt.tingtun.no'], 'obey-robotstxt': 'false'}
    >>> convert_config(cfg)
    {'url_limits': {'application/xml': 6000, 'text/html': 6000, 'application/xhtml+xml': 6000},
     'byte_limits': {'application/xml': 500, 'text/html': 500, 'application/xhtml+xml': 500},
     'url_filter': [('+', '^https?://utt\\.tingtun\\.no')], 'time_sleeptime': 2, 'flag_ignorerobots': True,
     'urls': ['http://www.tingtun.no', 'http://utt.tingtun.no']} 
    """

    config_dict = {}
    # Some values are directly mapped - only name change
    mapping_keys = {'seeds': 'urls',
                    'scoping-rules': 'url_filter',
                    'min-crawl-delay': 'time_sleeptime',
                    'loglevel': 'loglevel'
                    }

    other_keys = {'max-pages': 'url_limits',
                  'size-limits': 'byte_limits'}
    
    for key, new_key in mapping_keys.items():
        if key in crawler_rules:
            config_dict[new_key] = crawler_rules[key]

    for key, new_key in other_keys.items():
        val = crawler_rules[key]

        limit_dict = {}
        for entry in val:
            mime_types = entry[0]
            limit = entry[1]

            for mime_type in mime_types:
                limit_dict[mime_type] = int(limit)

        config_dict[new_key] = limit_dict
        
    config_dict['flag_ignorerobots'] = not crawler_rules['obey-robotstxt']

    return config_dict

if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)
