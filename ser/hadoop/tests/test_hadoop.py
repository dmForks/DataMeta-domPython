from __future__ import print_function

"""Tests for DataMeta Hadoop."""

import os
import sys
import pytest

sys.path.insert(0, os.path.abspath('.'))

import logging
logging.basicConfig(filename='hadoopTests.log', level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

from collections import *
from ebay_datameta_hadoop.base import *
from ebay_datameta_core.canned_re import CannedRe
from inspect import getmembers
import inspect
from pprint import *


# see examples here: https://github.com/jeffknupp/sandman2/tree/master/tests

# test if all the packages are accessible
def test_packaging():
    e = CannedRe.EMAIL
    assert e is not None

