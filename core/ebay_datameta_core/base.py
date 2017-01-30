#!/bin/env python

import abc
import calendar
import re
from datetime import datetime
from enum import Enum

# What happens if you try to create an instance of the interface (abstract class):
# >       v = Verifiable()
# E       TypeError: Can't instantiate abstract class Verifiable with abstract methods verify
# see https://pymotw.com/2/abc/


# noinspection PyClassHasNoInit
class DataMetaEntity:
    """
    Ancestor for all generated DataMeta DOM classes
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def getVersion(self):
        return


# noinspection PyClassHasNoInit
class Verifiable(DataMetaEntity):
    """
    Interface for the verify method.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def verify(self):
        return


# noinspection PyClassHasNoInit
class Migrator:
    """
    Interface for the migrators.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def migrate(self, src, *xtras):
        return


# FIXME This interface may not be needed because Python's __eq__ is overloaded with the == operator unlike Java's equals
class DataMetaSame:
    """
    Interface for the equality.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def is_same(self, one, another):
        return


class DateTime():

    FORMAT_WITH_TZ = "%Y-%m-%dT%H:%M:%S%Z"
    FORMAT = "%Y-%m-%dT%H:%M:%S"
    @staticmethod
    def fromMillis(ms):
        return datetime.utcfromtimestamp(ms // 1000)

    @staticmethod
    def toMillis(dt):
        return calendar.timegm(dt.timetuple()) * 1000

    @staticmethod
    def fromIsoUtc(s):
        # strip the final Z if it's there, it's useless in this case
        return datetime.strptime("%sUTC" % (s[:-1] if s.endswith("Z") else s), DateTime.FORMAT_WITH_TZ)

    @staticmethod
    def toIsoUtc(dt):
       return "%sUTC" % dt.strftime(DateTime.FORMAT) # adding %Z specs to this format does not render the TZ, must append it manually


# noinspection PyCompatibility
class SemVer():
    DiffLevel = Enum("DiffLevel", "NONE MAJOR MINOR UPDATE BUILD")
    # Split by dots pattern
    DOTS_SPLIT = re.compile(r"\.")
    DIGITS = re.compile(r"^[0-9]+$")

    # Major part index in the @items array
    MAJOR_INDEX = 0
    # Minor part index in the @items array
    MINOR_INDEX = MAJOR_INDEX + 1
    # Update part index in the @items array
    UPDATE_INDEX = MINOR_INDEX + 1
    # Build part index in the @items array
    BUILD_INDEX = UPDATE_INDEX + 1
    # Minimal size of the @items array
    ITEMS_MIN_SIZE = UPDATE_INDEX + 1
    # Max size of the @items array
    ITEMS_MAX_SIZE = BUILD_INDEX + 1

    # Equality for the __cmp__ method
    EQ = 0
    # Strictly "greater than" for the __cmp__ method
    GT = 1
    # Strictly "lesser than" for the __cmp__ method
    LT = -1

    def __init__(self, src):
        if src is None: raise AttributeError("Atempted to create an instance of %s from None" % self.__class__.__name__)
        self._source = src
        self._items = []
        for x, v in enumerate(re.split(SemVer.DOTS_SPLIT, self._source)):
            if re.match(SemVer.DIGITS, v):
                self._items.append(int(v))
            else:
                break

        if len(self._items) < SemVer.ITEMS_MIN_SIZE or len(self._items) > SemVer.ITEMS_MAX_SIZE:
            raise AttributeError("Invalid semantic version format: %s" % self._source)

        if self.build is None or self.build == 0:
            raise AttributeError("Invalid semantic version format: %s: build version can not be zero" % self._source)

        self._semanticPartsOnly = '.'.join(map(str, self._items))

# Consistently and reproducibly convert the version specs to the text suitable for making it a part of a class name or a
# variable name
    def toVarName(self):
        return '_'.join(map(str, self._items))

    def items(self):
        return self._items

    def semanticPartsOnly(self):
        return self._semanticPartsOnly

    def source(self):
        return self._source

    def major(self):
        return self._items[SemVer.MAJOR_INDEX]

    def minor(self):
        return self._items[SemVer.MINOR_INDEX]

    def update(self):
        return self._items[SemVer.UPDATE_INDEX]

    def build(self):
        return self._items[SemVer.BUILD_INDEX] if len(self._items) > SemVer.BUILD_INDEX else None

    def diffLevel(self, other):
        if self.major() != other.major(): return SemVer.DiffLevel.MAJOR
        if self.minor() != other.minor(): return SemVer.DiffLevel.MINOR
        if self.update() != other.update(): return SemVer.DiffLevel.UPDATE

        if(
                (self.build() is not None and other.build() is not None and cmp(self.build(), other.build()) != SemVer.EQ)
            or
                (self.build() is None and other.build() is not None)
            or
                (self.build() is not None and other.build() is None)
        ):
            return SemVer.DiffLevel.BUILD
        return SemVer.DiffLevel.NONE

    def __eq__(self, other):
        return self._items == other.items()

    def __hash__(self):
        return hash(str(self._items))

    def __cmp__(self, o):
        if o is None:
            raise AttributeError, "Attempt to compare %s (%s) to a None" % (self.__class__.__name__, self)

        for x in xrange(SemVer.ITEMS_MIN_SIZE):
            c = cmp(self._items[x], o.items()[x])
            if c != SemVer.EQ: return c

        if len(self._items) > len(o.items()): return SemVer.GT

        if len(self._items) < len(o.items()): return SemVer.LT

        return cmp(self.build(), o.build())

    def __str__(self):
        return self._source

    def longStr(self):
        return "%s{%s(%s}" %(self.__class__.__name__, self._source, self._semanticPartsOnly)


