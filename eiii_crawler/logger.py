# -- coding: utf-8

""" Generic logger module which can be used by any application
for logging. Wraps up Python's logging object in a convenient
LoggerWrapper class which provides wrapper functions for logging
at various levels using variable arguments.

"""

import logging
import functools
import re
import sys
import os
import time

# List of current loggers
__loggers__ = {}

# A custom level between debug and info
logging.EXTRA = 15

class LoggerWrapper(object):
    """ Wrapper class on logging object which provides
    convenience functions for logging at different levels """

    def __init__(self, log, console=False):
        self._log = log
        self._startt = time.time()
        # Debug flag - if set, can be used to trace the
        # source of the caller stack frame
        self._sdebug = False
        # Overridden functions
        self._supported = ('info','warning','error','critical','debug', 'extra')
        # Preceding and trailing single quotes
        self.squote_re = re.compile("^'|^u'|'$")
        # Create function wrappers for these
        for name in self._supported:
            # Creating convenient wrappers by using functools
            func = functools.partial(self._dolog, name)
            setattr(self, name, func)

        # Standard formatter
        self.stdformat = logging.Formatter('%(asctime)s [%(timespent)s] :%(levelname)-8s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        
        self.__console = False
        # Add console logging if specified
        if console:
            # Prepare console logging handlers
            self._prepareConsoleHandlers()
            self._addConsoleHandlers()
            
        self.__console = console
        # Additional file handle
        self._extrafhandle = None
        

    def myrepr(self, string):
        return self.squote_re.sub('',repr(string))

    def _prepareConsoleHandlers(self):
        """ Prepare console (stdout/stderr) logging handlers """
        
        self.shout = logging.StreamHandler(sys.stdout)
        self.sherr = logging.StreamHandler(sys.stderr)       
        # Formatter - skip debugging for console formatter
        formatter2 = logging.Formatter('%(asctime)s [%(timespent)s] :%(levelname)-8s - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S')
        self.shout.setFormatter(formatter2)
        self.sherr.setFormatter(formatter2)      
        # shout should filter all log records >= ERROR
        self.shout.addFilter(ReverseErrorFilter())
        # sherr should filter all log records < ERROR
        self.sherr.addFilter(ErrorFilter())

    def _addConsoleHandlers(self):
        """ Add console logging handlers to the logger """

        # If already set, do nothing
        if self.__console:
            return False

        self._log.addHandler(self.shout)
        self._log.addHandler(self.sherr)

    def _removeConsoleHandlers(self):
        """ Remove console logging handlers """

        if not self.__console:
            return False

        # Remove handlers
        try:
            self._log.handlers.remove(self.shout)
            self._log.handlers.remove(self.sherr)
            return True
        except ValueError:
            return False
        
    def _getMessage(self, msg, *args, **kwargs):
        """ Return message with variable arguments """

        try:
            return ' '.join([str(msg)] + map(lambda x: str(x), args))
        except (UnicodeEncodeError, UnicodeDecodeError), e:
            return ' '.join([self.myrepr(msg)] + map(lambda x: self.myrepr(x), args))           

    def _dolog(self, levelname, msg, *args, **kwargs):
        """ Generic function for logging with variable arguments """

        tnow = time.time()
        tdiff = int(round(tnow - self._startt))

        hr, rem = divmod(tdiff, 3600)
        mins, sec = divmod(rem, 60)
        tsofar = '%.2d:%.2d:%.2d' % (hr, mins, sec)
                         
        try:
            logfunc = getattr(self._log, levelname)
            # print self._log, id(self._log), self._log.level
            return logfunc(self._getMessage(msg, *args), extra={'timespent': tsofar})         
        except AttributeError:
            logfunc = getattr(self._log, 'info')
            level = eval('logging.' + levelname.upper())

            # Custom levels are < INFO
            if (self._log.level <= level):
                return logfunc(self._getMessage(msg, *args), extra={'timespent': tsofar})

    def logsimple(self, msg, *args):
        """ Send a log line to the log file with no formatting """

        # Save current formatting - for the time being all formatting
        # is the same so we save just the first one.
        formatter = self._log.handlers[0].formatter
        plain_formatter = logging.Formatter("%(message)s")
        for handler in self._log.handlers:
            handler.setFormatter(plain_formatter)

        # Log the message at info level
        self._log.info(self._getMessage(msg, *args))

        # Reset formatter
        for handler in self._log.handlers:
            handler.setFormatter(formatter)        

    def justlog(self, msg, *args, **kwargs):
        """ Send a log line justified by a fixed width of spaces.
        Logging is by default done at the INFO level when using
        this function """

        # Justify args
        justify = kwargs.get('justify', 40)
        # keychar to justify
        fillchar = kwargs.get('fillchar', '>')
        
        msgargs = self._getMessage('', *args)
        # spaces to justify
        nspace = justify - len(msg)

        if nspace>0:
            fullmsg = msg + " " + msgargs.rjust(nspace, fillchar)
        else:
            # Cant justify - msg too long
            fullmsg = self._getMessage(msg, *args)

        # Log the message at info level
        self._log.info(fullmsg, extra={'timespent': ''})         
        
    # Set debug flag
    def setDebug(self, val = True):
        """ Set the debug flag which allows to trace source code
        location for log emitters """

        # Note that this has nothing to do with the log level DEBUG
        self._sdebug = val

    def getDebug(self):
        """ Return the debug flag """
        
        return self._sdebug

    def setLevel(self, level):
        """ Set the logging level for the logger """

        # Allow clients to pass in levels as strings
        if type(level) is str:
            level = eval('logging.' + level.upper())

        self._log.setLevel(level)
        
        # Need to set level on each handler as well.
        for handler in self._log.handlers:
            # print 'Setting level',level,'for handler',handler
            handler.setLevel(level)     
    
    def setFormat(self, format):
        """ Set format for log lines """

        for handler in self._log.handlers:
            handler.setFormatter(logging.Formatter(format))

    def setConsole(self, val=True):
        """ Toggle console logging settings """

        if val:
            self._addConsoleHandlers()
        else:
            self._removeConsoleHandlers()

        self.__console = val

    def addLogFile(self, logfile, sethandler = True):
        """ Add a log file to the logger """

        # Remove previous file handle if any
        if self._extrafhandle != None:
            # print 'Removing file handle',self._extrafhandle
            self._log.removeHandler(self._extrafhandle)
            
        # print 'LOGFILE=>',logfile
        fh = logging.FileHandler(logfile)
        fh.setFormatter(self.stdformat)
        # add handler to logger object
        self._log.addHandler(fh)
        if sethandler: self._extrafhandle = fh

    def removeLogFile(self, logfile):
        """ Remove a log file from the logger """

        rhandle = None
        logfile = os.path.abspath(logfile)
        
        for handler in self._log.handlers:
            if type(handler) == logging.FileHandler:
                # print handler.baseFilename, logfile
                if handler.baseFilename == logfile:
                    # Remove it
                    rhandle = handler
                    break

        if rhandle != None:
            # print 'Removing file handler=>',rhandle
            self._log.removeHandler(rhandle)

class ErrorFilter(object):
    """ Logging filter class that filters out all
    log records at levels < ERROR """

    def filter(self, record):
        return int(record.levelno >= logging.ERROR)

class ReverseErrorFilter(object):
    """ Logging filter class that filters out all
    log records at levels >= ERROR """

    def filter(self, record):
        return int(record.levelno < logging.ERROR)

def getLogger(app, logfile, level=logging.INFO, console=False):
    """ Return a logger for the given app. Accepts a logfile
    and optional log level which defaults to INFO """

    # If a logger exists for the same app and logfile, return it
    key = (app, logfile)
    if key in __loggers__:
        return __loggers__.get(key)
    
    log = logging.getLogger(app)
    # Allow clients to pass in levels as strings
    if type(level) is str:
        level = eval('logging.' + level.upper())
        
    log.setLevel(level)

    logobject = LoggerWrapper(log, console=console)
    logobject.addLogFile(logfile, sethandler=False)

    # Make entry for the logging object in the logger dictionary
    __loggers__[key] = logobject

    return logobject

def getMultiLogger(app, logfile, errlogfile, level=logging.INFO, console=False):
    """ Return a logger for the given app in such a way that normal logging (levels
    of DEBUG, INFO, WARNING gets logged to the file 'logfile' and ERROR and above gets
    logged to the 'errlogfile'. This allows more fine-grained control over logging.

    Note that if both files are same, this function behaves same as getLogger.
    """

    if logfile==errlogfile:
        return getLogger(app, logfile, level, console=console)

    # If a logger exists for the same app and logfile, return it
    key = (app, logfile, errlogfile)
    if key in __loggers__:
        return __loggers__.get(key)
    
    log = logging.getLogger(app)

    # Allow clients to pass in levels as strings
    if type(level) is str:
        level = eval('logging.' + level.upper())
        
    log.setLevel(level)

    # Create two file handlers
    fhout = logging.FileHandler(logfile)
    fherr = logging.FileHandler(errlogfile) 

    formatter = logging.Formatter('%(asctime)s [%(timespent)s]: %(levelname)-8s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S') 

    fhout.setFormatter(formatter)
    fherr.setFormatter(formatter)

    # fhout should filter all log records >= ERROR
    fhout.addFilter(ReverseErrorFilter())
    # fherr should filter all log records < ERROR
    fherr.addFilter(ErrorFilter())
    
    # add handler to logger object
    log.addHandler(fhout)
    log.addHandler(fherr)

    logobject = LoggerWrapper(log, console=console)

    # Make entry for the logging object in the logger dictionary
    __loggers__[key] = logobject

    return logobject

if __name__ == "__main__":
    pass
