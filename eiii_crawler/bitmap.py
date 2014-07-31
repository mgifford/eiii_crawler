""" Class for keeping your own bit vectors allowing to
check presence of integers.

Borrowed from 'Programming Pearls' by Jon J Bentley.

"""
import unittest

class Bitmap(object):
    """ Efficient container to search integers """
    
    BITSPERWORD = 32
    SHIFT =  5
    MASK = 0x1F
    
    def __init__(self, maxn=2**16):
        """ Initializer - accepts maximum number that
        can be kept in bitmap """

        self.maxn = maxn
        self.bits = [0]*(1 + (self.maxn/self.BITSPERWORD))
        # print len(self.bits)
        # Clear all
        for i in range(self.maxn):
            self.clear(i)

    def clear(self, i):
        """ Clear bit position for integer 'i' """

        self.bits[i>>self.SHIFT] &= ~(1<<(i & self.MASK))

    def set(self, i):
        """ Set bit position for integer 'i' """
        
        self.bits[i>>self.SHIFT] |= (1<<(i & self.MASK))

    def test(self, i):
        """ Test bit position for integer 'i' """
        
        return self.bits[i>>self.SHIFT] & (1<<(i & self.MASK))>0

class TestBitmap(unittest.TestCase):
    """ Unit test case for Bitmap class """
    
    def setUp(self):
        self.bitmap = Bitmap()
        self.nums_set = [35, 132, 49, 1500, 2234]
        self.nums_not_set = [10, 15, 2000, 1455, 23]

    def test_set_and_get(self):
        """ Test set and get """
        
        for num in self.nums_set:
            self.bitmap.set(num)

        for num in self.nums_set:
            assert(self.bitmap.test(num) == True)

        for num in self.nums_not_set:
            assert(self.bitmap.test(num) == False)           
        
        
if __name__ == "__main__":
    unittest.main()

    
    
        
