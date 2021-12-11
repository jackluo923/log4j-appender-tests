package com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.intermediateRepresentation.aggregation;

import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.HashMapKey.ByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.intermediateRepresentation.BufferedFileOutputStreamIR;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public abstract class BaseAggregationOutputStreamIR extends BufferedFileOutputStreamIR implements AggregationIR {
    ObjectOpenCustomHashSet<Object> deduplicationHashSet;   // logtype and variable type can share the same set

    static class ByteArrayHashStrategy<K> implements Hash.Strategy<K> {
        @Override
        public int hashCode(Object o) {
            if (o instanceof byte[] byteArray) {
                return Arrays.hashCode(byteArray);
            } else {
                // The other type is always ByteArrayViewDictionaryKey or its child
                return o.hashCode();
            }
        }

        @Override
        public boolean equals(Object a, Object b) {
            // Always invoke the equals function on the ByteArrayViewDictionaryKey function
            if (a instanceof ByteArrayViewDictionaryKey key) {
                return key.equals(b);
            } else if (b instanceof ByteArrayViewDictionaryKey key) {
                return key.equals(a);
            }
            return false;
        }
    }

    public BaseAggregationOutputStreamIR(Path path, int compressionLevel) throws IOException {
        super(path, compressionLevel);
        deduplicationHashSet = new ObjectOpenCustomHashSet<>(new ByteArrayHashStrategy<>());
    }

    @Override
    public void putTimestamp(long timestamp) throws IOException {
        bufferedDataOutputStream.writeByte(0x10);
        bufferedDataOutputStream.writeLong(timestamp);
    }

    @Override
    public void putLogtype(ByteArrayViewDictionaryKey logtype) throws IOException {
        int viewSize = logtype.getViewSize();
        if (deduplicationHashSet.contains(logtype)) {
            if (viewSize <= Byte.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x21);
                bufferedDataOutputStream.writeByte(viewSize);
            } else if (viewSize <= Short.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x22);
                bufferedDataOutputStream.writeShort(viewSize);
            } else {
                bufferedDataOutputStream.writeByte(0x23);
                bufferedDataOutputStream.writeInt(viewSize);
            }
        } else {
            if (viewSize <= Byte.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x24);
                bufferedDataOutputStream.writeByte(viewSize);
            } else if (viewSize <= Short.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x25);
                bufferedDataOutputStream.writeShort(viewSize);
            } else {
                bufferedDataOutputStream.writeByte(0x26);
                bufferedDataOutputStream.writeInt(viewSize);
            }
        }
        bufferedDataOutputStream.write(logtype.bytes, logtype.beginIndex, logtype.endIndex);
    }

    public void putDictionaryVariable(ByteArrayViewDictionaryKey variable) throws IOException {
        int viewSize = variable.getViewSize();
        if (deduplicationHashSet.contains(variable)) {
            if (viewSize <= Byte.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x31);
                bufferedDataOutputStream.writeByte(viewSize);
            } else if (viewSize <= Short.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x32);
                bufferedDataOutputStream.writeShort(viewSize);
            } else {
                bufferedDataOutputStream.writeByte(0x33);
                bufferedDataOutputStream.writeInt(viewSize);
            }
        } else {
            if (viewSize <= Byte.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x34);
                bufferedDataOutputStream.writeByte(viewSize);
            } else if (viewSize <= Short.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x35);
                bufferedDataOutputStream.writeShort(viewSize);
            } else {
                bufferedDataOutputStream.writeByte(0x36);
                bufferedDataOutputStream.writeInt(viewSize);
            }
        }
        bufferedDataOutputStream.write(variable.bytes, variable.beginIndex, viewSize);
    }

    public abstract void putEncodedVariable(ByteArrayViewDictionaryKey variable) throws IOException;
}
