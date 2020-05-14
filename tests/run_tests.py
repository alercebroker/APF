import unittest
import os
import sys
FILE_PATH = os.path.dirname(os.path.abspath(__file__))

sys.path.append(FILE_PATH)
from consumers import *
from db import *
from producers import *

if __name__ == "__main__":
    unittest.main()
