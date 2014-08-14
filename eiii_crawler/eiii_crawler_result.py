""" Sample client for demonstrating how to talk with
the crawler server for the result """

from ttrpc.client import *
import sys

# Wait for 3 hours for crawl to end.
proxy = TTRPCProxy('tcp://localhost:8910', retries=1,
                   timeout=10800*1000)

# Set seeds
print proxy.getresult(sys.argv[1])


