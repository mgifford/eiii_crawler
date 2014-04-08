import inspect
import requests
from contextlib import contextmanager

# Global logger object
g_logger = None

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
