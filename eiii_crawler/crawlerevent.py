""" Classes for managing decoupled event signalling mechanism for the crawler framework

Event management for crawling - this implements a simple decoupled Publisher-Subscriber
design pattern through a mediator whereby specific events are raised during the crawler workflow.
Client objects can subscribe to these events by passing a function that should be called
when the event occurs. The mediator keeps a handle of the functions. When an event occurs
the object that raises the event does so by calling "publish(publisher, event_name, **kwargs)" on the
mediator. The mediator creates an Event object from the keyword arguments and notifies all
the subscribers to that event by invoking their functions they have registerd with the
event object as argument.

"""

import datetime
import utils
import collections
import uuid

# Default logging object
log = utils.get_default_logger()
        
class CrawlerEvent(object):
    """ Class describing a crawler event raised by publishers
    during crawler workflow """

    def __init__(self, publisher, event_name, event_key=None, source=None, message=None,
                 message_type='str',code=0, is_error=False, is_critical=False, 
                 callback=None,params={}):
        # Object that publishes the event. This should
        # never be Null.
        self.publisher = publisher
        # Event published for
        self.event_name = event_name
        # The function or method that raises the event.
        # This can be Null.
        self.source = source
        # Time of publishing
        self.timestamp = datetime.datetime.now()
        # Message string if any. This can be an
        # error message for communicating situations
        # with errors.
        self.message = message
        self.message_type = message_type
        # The message_type is used to 'massage' the message
        # into information
        # Code if any - this can be an error code
        # for communicating situations with errors
        self.code = code
        # Is this an error situation ?
        self.is_error = is_error
        # Indicates an urgent situation
        self.is_critical = is_critical
        # Indicates a callback method (object, not name)
        # that can give more information - this can be Null
        # If not null, the callback should accept the publisher
        # and the same keyword arguments that was sent along
        # with the event as arguments.
        self.callback = callback
        # Dictionary of additional information which is
        # mostly understood only by the subscriber method
        # (Protocol between publisher and subscriber)
        self.params = params
        # ID of the event
        self.id = uuid.uuid4().hex

        # NOTE: The publisher should at least provide the
        # publisher instance itself and a message.
        self._massage()

    def _massage(self):
        """ Massage the message into information """

        try:
            self.data = eval("%s('%s')" % (self.message_type, self.message))
        except Exception, e:
            self.data = ''
            log.error(str(e))

    def __str__(self):
        return 'Event for "%s", published at [%s] - id <%s>' % (self.event_name,
                                                              self.timestamp,
                                                              self.id)

class CrawlerEventRegistry(object):
    """ Event mediator class which allows subscribers to listen to published
    events from event publishers and take actions accordingly """

    # This class is a Singleton
    __metaclass__ = utils.SingletonMeta
    # Dictionary of allowed event names and descriptions
    __events__ = {'download_complete': 'Published when a URL is downloaded successfully',
                  'download_complete_fake': 'Published when a URL is fetched using HEAD request only',
                  'download_complete_cheat': 'Published when download of a URL is simulated completely',
                  'download_error': 'Published when a URL fails to download',
                  'download_cache': 'Retrieved a URL from the cache',
                  'url_obtained': 'Published when a URL is obtained from the pipeline for processing',
                  'url_pushed': 'Published when a new URL is pushed to the pipeline for processing',
                  'url_parsed': "Published after a URL's data has been parsed for new (child) URLs",
                  'url_filtered': "Published when a URL has been filtered after applying a rule",
                  'crawl_started': "Published when the crawl is started, no events can be published before this event",
                  'crawl_ended': "Published when the crawl ends, no events can be published after this event",
                  'abort_crawling': "Published if the crawl has to be aborted midway"
                  # More to come
                  }
     
    def __init__(self):
        # Dictionary of subscriber objects and the method
        # to invoke mapped against the event name as key
        # A key is mapped to a list of subscribers where each
        # entry in the list is the subscriber method (method
        # object, not name)
        self.subscribers = collections.defaultdict(set)
        # Dictionary of events published mapped by unique key
        self.unique_events = {}

    def reset(self):
        """ Reset state """
        self.unique_events = {}
        
    def publish(self, publisher, event_name, **kwargs):
        """ API called by the event publisher to announce an event.
        The required arguments for creating the Event object should
        be passed as keyword arguments. The first argument is always
        a handle to the publisher object itself """

        # Not confirming to the list of supported events ? 
        if event_name not in self.__events__:
            log.info('Unrecognized event =>',event_name)
            return False

        event_key = kwargs.get('event_key')
        # print 'EVENT KEY=>',event_key,event_name
        
        # If key is given and event already published, don't publish anymore.
        if event_key != None:
            # Create unique key => (event_key, event_name)
            key = (event_key, event_name)
            if key in self.unique_events:
                log.info("Not publishing event =>", key)
                return False
            else:
                # Add it
                self.unique_events[key] = 1
            
        # Create event object
        event = CrawlerEvent(publisher, event_name, **kwargs)
        # Notify all subscribers
        return self.notify(event)
        
    def notify(self, event):
        """ Notify all subscribers subscribed to an event """
        
        # Find subscribers
        # log.debug('Notifying all subscribers...',event)

        # print self.subscribers
        
        count = 0
        for sub in self.subscribers.get(event.event_name, []):
            # log.info("Calling subscribers for =>",event.event_name, '=>', sub)
            sub(event)
            count += 1

        return count

    def subscribe(self, event_name, method):
        """ Subscribe to an event with the given method """

        self.subscribers[event_name].add(method)

