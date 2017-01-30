from __future__ import print_function

"""Tests for DataMeta DOM."""
import os
import sys
import pytest

sys.path.insert(0, os.path.abspath('.'))

from collections import *

import re

from ebay_datameta_core.canned_re import CannedRe
from ebay_datameta_core.base import Verifiable, DateTime, Migrator, SemVer
from test_ebay_datameta_sample_v3.model import *

#from inspect import getmembers

# see examples here: https://github.com/jeffknupp/sandman2/tree/master/tests
# Not just tests but also Python case studies that did reveal some unexpected discoveries,
# such as the code in property settings not run. Apparently, in Python, some things do
# work as documented, some do not.

def test_good_phone():
    """If a valid phone number passes the canned test as it should?"""
    good_phone = "213-555-1212"
    m = CannedRe.PHONE.match(good_phone)
#    print getmembers(m)
    assert m is not None, "Canned RegEx phone test failed for %s" % good_phone
    assert m.string == good_phone


def test_dt_roundtrip():
    ms = 1464586777000
    dt = DateTime.fromMillis(ms)
    assert ms == DateTime.toMillis(dt)


def test_bad_phone():
    """If an invalid phone number passes the canned test as it should not?"""
    bad_phone = "213-555-121"
    m = CannedRe.PHONE.match(bad_phone)
    assert m is None, "Canned RegEx phone test succeeded for %s while it should not" % bad_phone


def test_good_email():
    """If a valid email address passes the canned test as it should?"""
    good_email = "me@dom.com"
    m = CannedRe.EMAIL.match(good_email)
    #    print getmembers(m)
    assert m is not None, "Canned RegEx email test failed for %s" % good_email
    assert m.string == good_email


def test_bad_email():
    """If an invalid email number passes the canned test as it should not?"""
    bad_email = "me.dom.com"
    m = CannedRe.EMAIL.match(bad_email)
    assert m is None, "Canned RegEx email test succeeded for %s while it should not" % bad_email


# noinspection PyUnusedLocal,PyUnresolvedReferences
def test_verifiable_abstract():
    with pytest.raises(TypeError):
        v = Verifiable()


# noinspection PyUnusedLocal,PyUnresolvedReferences
def test_semVerParsing():
    with pytest.raises(AttributeError): v = SemVer(".2.3")
    with pytest.raises(AttributeError): v = SemVer("1.2.")
    with pytest.raises(AttributeError): v = SemVer("1a.2.3")
    with pytest.raises(AttributeError): v = SemVer("1.2a.3")
    with pytest.raises(AttributeError): v = SemVer("1.2.3a")
    with pytest.raises(AttributeError): v = SemVer("1.2.a3")
    with pytest.raises(AttributeError): v = SemVer("")
    with pytest.raises(AttributeError): v = SemVer(None)
    with pytest.raises(AttributeError): v = SemVer("-1.2.3")
    with pytest.raises(AttributeError): v = SemVer("1.-2.3")
    with pytest.raises(AttributeError): v = SemVer("1.2.-3")

    assert SemVer('12.234.456').toVarName() == '12_234_456'
    assert SemVer('12.234.456.7890').toVarName() == '12_234_456_7890'
    assert SemVer('12.234.456.7890.blah.yada.meh').toVarName() == '12_234_456_7890'

    def assertSemVer(ver, maj, minr, upd, bld):
        assert ver.major() == maj
        assert ver.minor() == minr
        assert ver.update() == upd
        assert ver.build() == bld

    assertSemVer(SemVer('12.345.6.7'), 12, 345, 6, 7)
    assertSemVer(SemVer('12.345.6.7.blah-blah-yada.yada'), 12, 345, 6, 7)
    assertSemVer(SemVer('12.345.6'), 12, 345, 6, None)
    assertSemVer(SemVer('12.345.6.blah-blah-yada.yada'), 12, 345, 6, None)


def test_semVerCmp():
    v1 = SemVer('5.6.7')
    v2 = SemVer('12.15.16')
    assert v1.source() > v2.source()
    assert v1 < v2

    v1 = SemVer('5.6.7.8')
    v2 = SemVer('5.6.7')
    assert v1.source() > v2.source()
    assert v1 > v2

    v1 = SemVer('5.6.7')
    v2 = SemVer('5.6.7.8')
    assert v1.source() < v2.source()
    assert v1 < v2

    v1 = SemVer('5.6.7.3')
    v2 = SemVer('5.6.7.12')
    assert v1.source() > v2.source()
    assert v1 < v2

    v1 = SemVer('5.6.7.8')
    v2 = SemVer('5.6.7.8')
    assert v1.source() == v2.source()
    assert v1 == v2


def test_diffLevel():
    assert SemVer('1.2.3').diffLevel(SemVer('1.2.3.blah')) == SemVer.DiffLevel.NONE
    assert SemVer('1.2.3.4').diffLevel(SemVer('1.2.3.4.blah')) == SemVer.DiffLevel.NONE
    assert SemVer('1.2.3').diffLevel(SemVer('2.2.3.blah')) == SemVer.DiffLevel.MAJOR
    assert SemVer('1.2.3').diffLevel(SemVer('1.4.3.blah')) == SemVer.DiffLevel.MINOR
    assert SemVer('1.2.3').diffLevel(SemVer('1.2.4.blah')) == SemVer.DiffLevel.UPDATE
    assert SemVer('1.2.3.4').diffLevel(SemVer('1.2.3.blah')) == SemVer.DiffLevel.BUILD
    assert SemVer('1.2.3').diffLevel(SemVer('1.2.3.4.blah')) == SemVer.DiffLevel.BUILD
    assert SemVer('1.2.3.4').diffLevel(SemVer('1.2.3.5.blah')) == SemVer.DiffLevel.BUILD

# noinspection PyUnusedLocal,PyUnresolvedReferences
def test_migrator_abstract():
    with pytest.raises(TypeError):
        v = Migrator()


class GetSet: # same results as with Props, but no code except of the setter runs. Exception not raised,
    # value not printed.
    # Use this technique!
    def __init__(self):
        self.__x = None # better initialize all to None, otherwise would need to check __dict__, much more work.

    def getX(self):
        return self.__x

    def setX(self, val):
        print("==== GetSet: Setter val=%s" % val)
        if val is None: raise AttributeError("attribute \"x\" is required")
        self.__x = val


class Props: # can not use that. No extra code in setters run at all
    def __init__(self):
        self.__x = None

    @property
    def x(self):
        return self.__x

    @x.setter
    def x(self, val):
        print("***** Props: Setter val=%s" % val)
        if val is None: raise AttributeError("attribute \"x\" is required")
        # even unconditional does not run, test still passed
        raise AttributeError("attribute \"x\" is required")
        # but this does.
        self.__x = val


def test_getSet():
    gs = GetSet()
    print("Fresh GetSet X: %s" % gs.getX())
    gs.setX("First value")
    print("1st val GetSet X: %s" % gs.getX())
    gs.setX("Second value")
    print("2nd val GetSet X: %s" % gs.getX())
    gs.__x = "Forced value"
    print("Forced val GetSet X: %s" % gs.getX())
    print("Yanked val GetSet X: %s" % gs.__x)
    print("Class-augmented val GetSet X: %s" % gs._GetSet__x)
# this fails with the exception as it should and prints the value as it should:
#    gs.setX(None)
#    assert gs is None


def test_getProps():
    ps = Props()
    print("Fresh Props X: %s" % ps.x)
    ps.x = "First value"
    print("1st val Props X: %s" % ps.x)
    ps.x = "Second value"
    print("2nd val Props X: %s" % ps.x)
    ps.__x = "Forced value"
    print("Forced val Props X: %s" % ps.x)
    print("Yanked val Props X: %s" % ps.__x)
# this, apparently, does not fail and does not print:
    ps.x = None
#   assert ps is None


def test_enumWorded():
    w1 = WordedEnum['VarString']
    w2 = WordedEnum(4)
    w3 = WordedEnum.VarString
    assert w1 == w2
    assert w1 == w3


def test_enumColor():
    i = IdLess()
    e1 = BaseColor['Green']
    e2 = BaseColor(2)
    e3 = BaseColor.Green
    assert e1 == e2
    assert e1 == e3


def test_kitchenSinkEq():
    k1 = getKitchenSink()
    k1.verify()
    k2 = getKitchenSink()
    k2.verify()
    assert k1 == k2
    k2.setTemperature(-273.15)
    assert k1 != k2


def getKitchenSink():
    k = KitchenSink()
    setOfStrings = {"one", "two", "three"}
    k.setId(1)
    k.setContext("KitchenKtx")
    k.setStrings(setOfStrings)
    embo1 = Embodiment()
    embo1.setId(100)
    embo1.setInclusivement("one hundred")
    embo2 = Embodiment()
    embo2.setId(200)
    embo2.setInclusivement("twice hundred")

    embT1 = EmbeddedType()
    embT1.setIntCode(10)
    embT1.setTxtCode("Textual One")
    embo1.setMbe(embT1)

    embT2 = EmbeddedType()
    embT2.setIntCode(20)
    embT2.setTxtCode("Textual Code Two")
    embo2.setMbe(embT2)

    k.setEmbeds([embT1, embT2])

    k.setInts(deque([1, 2, 3, 4, 5]))

    k.setTimes([DateTime.fromIsoUtc("2016-04-23T12:40:04Z"), DateTime.fromIsoUtc("2016-02-29T22:50:54Z")])

    idLess1 = IdLess()
    idLess1.setCount(300)
    idLess1.setWhen(DateTime.fromIsoUtc("2016-03-31T15:33:44Z"))
    idLess2 = IdLess()
    idLess2.setCount(400)
    idLess2.setWhen(DateTime.fromIsoUtc("2014-11-07T15:33:44Z"))
    k.setIdLessNess({idLess1, idLess2})

    k.setStrToInt({"one": 1, "two": 2, "three": 3})
    k.setEmbToString({embT1: "one", embT2: "two"})
    k.setLongToEmb({1: embT1, 2: embT2})
    k.setEmbToEmb({embT1: embo2, embT2: embo1})
    k.setName("What's in the name")
    k.setCode("encoded!")
    k.setType(WordedEnum.Bitset)
    k.setChoices(None)
    k.setLength(1234)
    k.setFrequency(0xFFFFFFFFFF)
    k.setWeight(123.45)
    k.setTemperature(73.12)
    k.setIsRequired(True)
    k.setIsMeasurable(False)
    k.setComments("Everything is hunky-dory!")
    k.setHomePage("http://www.google.com:8080/blah/meh?v=a&a=v")
    k.setWorkPage("https://www.microsoft.com")
    k.setHomeEmail("me@domain.com")
    k.setHomeZip("12345-6789")
    k.setWorkZip("98765-4321")
    k.setEmbo(embo1)
    nsRec = ExampleNsRec()

    nsRec.setId(1000)
    nsRec.setName("John Doe")
    nsRec.setChoices(WordedEnum.Long)
    k.setOtherNsRef(nsRec)
    return k
