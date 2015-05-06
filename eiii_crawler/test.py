import multiprocessing
import threading
import signal
import sys
import os

class C(object):

    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.shared_dict = self.manager.dict()
        self.shared_dict2 = self.manager.dict()
        pass

    def sighandler(self, signum, stack):
        sys.exit(0)

if __name__ == "__main__":
    c = C()
    print os.getpid()
    signal.signal(signal.SIGTERM, c.sighandler)
    
    import time
    print 'waiting...'
    time.sleep(1000)
