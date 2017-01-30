#!/bin/env python

import abc

from hadoop.io import WritableUtils, InputStream, OutputStream, Text
from ebay_datameta_core.base import DateTime
from decimal import *
from collections import *
from bitarray import bitarray

from numpy import uint64, int64

class InOutable:

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def read(self, di):
        return

    @abc.abstractmethod
    def readVal(self, di, val):
        return

    @abc.abstractmethod
    def write(self, do, val):
        return

    @staticmethod
    def writeVersion(do, ver):
        WritableUtils.writeVInt(do, ver)

    @staticmethod
    def readVersion(di):
        return WritableUtils.readVInt(di)


class BytesDataIoUtil:

    @staticmethod
    def read(ba, io): # byte array and InOutable
        return io.read(InputStream.DataInputStream(InputStream.ByteArrayInputStream(ba)))

    @staticmethod
    def write(io, val):
        bo = OutputStream.ByteArrayOutputStream()
        do = OutputStream.DataOutputStream(bo)
        io.write(do, val)
        return bo.toByteArray()

    @staticmethod
    def readVersioned(ba, io): # byte array and InOutable
        di = InputStream.DataInputStream(InputStream.ByteArrayInputStream(ba))
        InOutable.readVersion(di)
        return io.read(di)

    @staticmethod
    def writeVersioned(io, val):
        bo = OutputStream.ByteArrayOutputStream()
        do = OutputStream.DataOutputStream(bo)
        InOutable.writeVersion(do, val.getVersion())
        io.write(do, val)
        return bo.toByteArray()


class DataMetaHadoopUtil:

    TZ_ID_TO_KEY = {} # leave this for future implementation, when timezones actually implemented in Python
    KEY_TO_TZ_ID = {} # for now, we restrict datetimes to UTC
    Z_TZ_ID = 0 # this is in sync with Java, Z timezone which is UTC

    @staticmethod
    def setTextualIfAny(txt, source): # Text and String
        txt.set("" if source is None else ("%s" % source))

    @staticmethod
    def writeTextIfAny(do, source): # DataOutput and String
        Text.writeString(do, "" if source is None else source)

    @staticmethod
    def readText(di):
        return Text.readString(di)

    @staticmethod
    def writeDttm(do, dttm): # DataOutput and DateTime
        WritableUtils.writeVInt(do, DataMetaHadoopUtil.Z_TZ_ID)
        WritableUtils.writeVLong(do, DateTime.toMillis(dttm))

    @staticmethod
    def readDttm(di):
        WritableUtils.readVInt(di) # timezone, discard it
        return DateTime.fromMillis(WritableUtils.readVLong(di))

    @staticmethod
    def writeDttmUtc(do, dttm): # DataOutput and DateTime
        """
        Saving with UTC saves one byte of a time zone and relieves the headache of maintaining one.
        Since all the dates are UTC and there is no TZ key, it makes easy to sort them.
        """
        WritableUtils.writeVLong(do, DateTime.toMillis(dttm))

    @staticmethod
    def readDttmUtc(di):
        return DateTime.fromMillis(WritableUtils.readVLong(di))

    @staticmethod
    def readBigDecimal(di):
        return Decimal(DataMetaHadoopUtil.readText(di))

    @staticmethod
    def writeBigDecimal(do, val):
        DataMetaHadoopUtil.writeTextIfAny(do, "%s" % val)

    @staticmethod
    def writeLongArray(do, array):
        WritableUtils.writeVInt(do, len(array))
        for v in array:
            WritableUtils.writeVLong(do, v)

    @staticmethod
    def readLongArray(di):
        n = WritableUtils.readVInt(di)
        result = []
        for i in range(n):
            result.append(WritableUtils.readVLong(di))
        return result

    @staticmethod
    def writeCollection(val, do, io):
        if val is not None: #if it is null, then the nullFlags had been set, don't need to do anything
            WritableUtils.writeVInt(do, len(val))
            for e in val:
                io.write(do, e)

    @staticmethod
    def readList(di, io):
        n = WritableUtils.readVInt(di)
        result = []
        for i in range(n):
            result.append(io.read(di))

        return result

    @staticmethod
    def readSet(di, io):
        n = WritableUtils.readVInt(di)
        result = set()
        for i in range(n):
            result.add(io.read(di))

        return result

    @staticmethod
    def readDeque(di, io):
        n = WritableUtils.readVInt(di)
        result = deque()
        for i in range(n):
            result.append(io.read(di))

        return result
    
####################### Primitive Lists ************************    
    
    @staticmethod
    def readListInteger(di):
        n = WritableUtils.readVInt(di) 
        result = []
        for i in range(n):
            result.append(WritableUtils.readVInt(di))
        return result    
           
    @staticmethod
    def readListLong(di):
        n = WritableUtils.readVInt(di)
        result = []
        for i in range(n):
            result.append(WritableUtils.readVLong(di))
        return result

    @staticmethod
    def readListBoolean(di):
        n = WritableUtils.readVInt(di)
        result = []
        for i in range(n):
            result.append(di.readBoolean())
        return result

    @staticmethod
    def readListFloat(di):
        n = WritableUtils.readVInt(di)
        result = []
        for i in range(n):
            result.append(di.readFloat())
        return result

    @staticmethod
    def readListDouble(di):
        n = WritableUtils.readVInt(di)
        result = []
        for i in range(n):
            result.append(di.readDouble())
        return result

    @staticmethod
    def readListString(di):
        n = WritableUtils.readVInt(di)
        result = []
        for i in range(n):
            result.append(DataMetaHadoopUtil.readText(di))
        return result

    @staticmethod
    def readListDateTime(di):
        n = WritableUtils.readVInt(di)
        result = []
        for i in range(n):
            result.append(DataMetaHadoopUtil.readDttm(di))
        return result

    @staticmethod
    def readListBigDecimal(di):
        n = WritableUtils.readVInt(di)
        result = []
        for i in range(n):
            result.append(DataMetaHadoopUtil.readBigDecimal(di))
        return result

    @staticmethod
    def writeListInteger(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                WritableUtils.writeVInt(do, e)
                
    @staticmethod
    def writeListLong(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                WritableUtils.writeVLong(do, e)

    @staticmethod
    def writeListBoolean(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                do.writeBoolean(e)

    @staticmethod
    def writeListFloat(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                do.writeFloat(e)

    @staticmethod
    def writeListDouble(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                do.writeDouble(e)

    @staticmethod
    def writeListString(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                DataMetaHadoopUtil.writeTextIfAny(do, e)

    @staticmethod
    def writeListDateTime(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                DataMetaHadoopUtil.writeDttm(do, e)

    @staticmethod
    def writeListBigDecimal(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                DataMetaHadoopUtil.writeBigDecimal(do, e)
            
############### Primitive Deques:


    @staticmethod
    def readDequeInteger(di):
        n = WritableUtils.readVInt(di)
        result = deque()
        for i in range(n):
            result.append(WritableUtils.readVInt(di))
        return result

    @staticmethod
    def readDequeLong(di):
        n = WritableUtils.readVInt(di)
        result = deque()
        for i in range(n):
            result.append(WritableUtils.readVLong(di))
        return result

    @staticmethod
    def readDequeBoolean(di):
        n = WritableUtils.readVInt(di)
        result = deque()
        for i in range(n):
            result.append(di.readBoolean())
        return result

    @staticmethod
    def readDequeFloat(di):
        n = WritableUtils.readVInt(di)
        result = deque()
        for i in range(n):
            result.append(di.readFloat())
        return result

    @staticmethod
    def readDequeDouble(di):
        n = WritableUtils.readVInt(di)
        result = deque()
        for i in range(n):
            result.append(di.readDouble())
        return result

    @staticmethod
    def readDequeString(di):
        n = WritableUtils.readVInt(di)
        result = deque()
        for i in range(n):
            result.append(DataMetaHadoopUtil.readText(di))
        return result

    @staticmethod
    def readDequeDateTime(di):
        n = WritableUtils.readVInt(di)
        result = deque()
        for i in range(n):
            result.append(DataMetaHadoopUtil.readDttm(di))
        return result

    @staticmethod
    def readDequeBigDecimal(di):
        n = WritableUtils.readVInt(di)
        result = deque()
        for i in range(n):
            result.append(DataMetaHadoopUtil.readBigDecimal(di))
        return result

    @staticmethod
    def writeDequeInteger(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                WritableUtils.writeVInt(do, e)

    @staticmethod
    def writeDequeLong(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                WritableUtils.writeVLong(do, e)

    @staticmethod
    def writeDequeBoolean(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                do.writeBoolean(e)

    @staticmethod
    def writeDequeFloat(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                do.writeFloat(e)

    @staticmethod
    def writeDequeDouble(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                do.writeDouble(e)

    @staticmethod
    def writeDequeString(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                DataMetaHadoopUtil.writeTextIfAny(do, e)

    @staticmethod
    def writeDequeDateTime(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                DataMetaHadoopUtil.writeDttm(do, e)

    @staticmethod
    def writeDequeBigDecimal(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                DataMetaHadoopUtil.writeBigDecimal(do, e)

############### Primitive Sets:

    @staticmethod
    def readSetInteger(di):
        n = WritableUtils.readVInt(di)
        result = set()
        for i in range(n):
            result.add(WritableUtils.readVInt(di))
        return result

    @staticmethod
    def readSetLong(di):
        n = WritableUtils.readVInt(di)
        result = set()
        for i in range(n):
            result.add(WritableUtils.readVLong(di))
        return result

    @staticmethod
    def readSetBoolean(di):
        n = WritableUtils.readVInt(di)
        result = set()
        for i in range(n):
            result.add(di.readBoolean())
        return result

    @staticmethod
    def readSetFloat(di):
        n = WritableUtils.readVInt(di)
        result = set()
        for i in range(n):
            result.add(di.readFloat())
        return result

    @staticmethod
    def readSetDouble(di):
        n = WritableUtils.readVInt(di)
        result = set()
        for i in range(n):
            result.add(di.readDouble())
        return result

    @staticmethod
    def readSetString(di):
        n = WritableUtils.readVInt(di)
        result = set()
        for i in range(n):
            result.add(DataMetaHadoopUtil.readText(di))
        return result

    @staticmethod
    def readSetDateTime(di):
        n = WritableUtils.readVInt(di)
        result = set()
        for i in range(n):
            result.add(DataMetaHadoopUtil.readDttm(di))
        return result

    @staticmethod
    def readSetBigDecimal(di):
        n = WritableUtils.readVInt(di)
        result = set()
        for i in range(n):
            result.add(DataMetaHadoopUtil.readBigDecimal(di))
        return result

    @staticmethod
    def writeSetInteger(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                WritableUtils.writeVInt(do, e)

    @staticmethod
    def writeSetLong(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                WritableUtils.writeVLong(do, e)

    @staticmethod
    def writeSetBoolean(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                do.writeBoolean(e)

    @staticmethod
    def writeSetFloat(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                do.writeFloat(e)

    @staticmethod
    def writeSetDouble(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                do.writeDouble(e)

    @staticmethod
    def writeSetString(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                DataMetaHadoopUtil.writeTextIfAny(do, e)

    @staticmethod
    def writeSetDateTime(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                DataMetaHadoopUtil.writeDttm(do, e)

    @staticmethod
    def writeSetBigDecimal(do, vals):
        if vals is not None:
            WritableUtils.writeVInt(do, len(vals))
            for e in vals:
                DataMetaHadoopUtil.writeBigDecimal(do, e)

    @staticmethod
    def bitArrayToLongs(ba): # bitarray
        longs = [] # resuling array of longs
        bitLen = len(ba) # length in bits
        i8Len = bitLen / 64 # length in Longs
        print("BL=%d, i8L =%d, mod=%d" %(bitLen, i8Len, bitLen % 64))

        if bitLen % 64 != 0: i8Len += 1
        print("BL=%d, i8L =%d, mod=%d" %(bitLen, i8Len, bitLen % 64))

        ulo = uint64(0)
        for ix in range(bitLen):
            loIx = uint64(ix % 64)
            if ba[ix]: ulo |= (uint64(1) << loIx)
            if loIx >= 63:
                longs.append(ulo.astype(int64))
                ulo = uint64(0)
        if bitLen ^ 64 != 0: longs.append(ulo.astype(int64))
        return longs

    @staticmethod
    def longsToBitArray(longs):
        loLen = len(longs)
        baLen = loLen * 64
        ba = bitarray(baLen)
        ba.setall(False)
        for ixL in range(loLen):
            ulo = uint64(longs[ixL])
            for ixB in range(64):
                if ulo & (uint64(1) << uint64(ixB)): ba[ixL * 64 + ixB] = True
        return ba

    @staticmethod
    def writeBitArray(do, ba):
        DataMetaHadoopUtil.writeLongArray(do, DataMetaHadoopUtil.bitArrayToLongs(ba))

    @staticmethod
    def readBitArray(di):
        return DataMetaHadoopUtil.longsToBitArray(DataMetaHadoopUtil.readLongArray(di))

