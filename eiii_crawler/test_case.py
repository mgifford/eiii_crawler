#!/usr/bin/env python

from ttrpc.server import SimpleTTRPCServer, UserError
from ttrpc.client import TTRPCProxy

import time
import datetime
import sys
import os
import gc
import signal
import multiprocessing
import threading
import argparse
import traceback
import zmq

class EIIICrawlerServer(SimpleTTRPCServer):
    """ EIII crawler server obeying the tt-rpc protocol """

    def __init__(self, nprocs=10, loglevel='info',bus_uri=None,port=8910,bind_addr='127.0.0.1'):
        # All the crawler objects
        self.instances = []
        # Number of tasks
        self.ntasks = 0
        # Tasks queue
        self.task_queue = multiprocessing.Queue()
        # Dictionary used to share return-values from
        # crawler processing
        self.manager = multiprocessing.Manager()
        # Return shared state dictionary shared with crawler processes
        self.return_dict = self.manager.dict()
        # Shared state - indicates crawler activity
        self.state = self.manager.dict()
        # Maxium number of crawl instances
        self.nprocs = nprocs
        # Log level
        self.loglevel = loglevel

        SimpleTTRPCServer.__init__(self)
        
        self.init_crawler_procs()
        
    def sighandler(self, signum, stack):
        """ Signal handler """

        print 'Killing self',os.getpid()
        
        for crawler in self.instances:
            print "Stopping Crawler",crawler.pid
            crawler.terminate()
            time.sleep(1)
            
            while crawler.is_alive():
                print '.',
                time.sleep(1)

        self.task_queue.close()
        print 'done.'
        
        # Send another TERM + KILL signal to myself!
        #os.kill(os.getpid(), signal.SIGTERM)
        #os.kill(os.getpid(), signal.SIGKILL)
        

    def init_crawler_procs(self):
        """ Start a pool of crawler processes """

        print 'Initializing',self.nprocs,'crawlers...'

        for i in range(self.nprocs):
            # Make a new instance
            print i
            
        print 'Initialized',self.nprocs,'crawlers.'

if __name__ == "__main__":
    crawler_rules = {'max-pages': [(['text/html', 'application/xhtml+xml', 'application/xml'], 50)],
                     'scoping-rules': [('+', '^https?://utt\\.tingtun\\.no')], 'min-crawl-delay': 2,
                     'size-limits': [(['text/html', 'application/xhtml+xml', 'application/xml'], 500)],
                     'seeds': ['http://docs.python.org/library/'], 'obey-robotstxt': 'false',
                     'loglevel': 'debug'}

    # You can given --nprocs as a command-line argument
    #
    # An equivalent number (or greater) of processes need to be
    # started on the tt-rpc server for proper 1:1 scaling.
    parser = argparse.ArgumentParser(description='EIII crawler server')
    parser.add_argument('--nprocs', dest='nprocs', default=10,type=int,
                        help='Number of crawler processes to start')
    parser.add_argument('--port', dest='port', default=8910,type=int,
                        help='Port number on which to listen')
    parser.add_argument('--debug', dest='loglevel', const='debug',
            default='info', nargs='?', help='Enable debug logging')
    parser.add_argument('--bus', dest='bus_uri', default=None, type=str,
                        help='URI to bus to register on.')
    parser.add_argument('--bindaddr', dest='bind_addr', default='127.0.0.1', type=str,
                        help='IP address on which to listen.')
    args = parser.parse_args()
    print 'Number of parallel crawler processes set to',args.nprocs
    print 'Starting crawler server on port',args.port,'...'
    if args.loglevel is 'debug':
        print 'Enabled debug messages.'
    if args.bus_uri:
        print 'Will register on bus at', args.bus_uri

    server = EIIICrawlerServer(nprocs=args.nprocs,loglevel=args.loglevel,
                               bus_uri=args.bus_uri, port=args.port, bind_addr=args.bind_addr
                               )

    signal.signal(signal.SIGTERM, server.sighandler)
    print 'PID is',os.getpid()
    # time.sleep(20000)

    try:
        server.listen("tcp://%s:%d" % (args.bind_addr, args.port), nprocs=args.nprocs*2)
    except zmq.error.ZMQError, e:
        print e
        sys.exit(1)    

    
