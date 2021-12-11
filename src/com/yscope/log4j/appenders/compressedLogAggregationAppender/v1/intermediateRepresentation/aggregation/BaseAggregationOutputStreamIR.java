package com.yscope.log4j.appenders.compressedLogAggregationAppender.v1.intermediateRepresentation.aggregation;

import com.yscope.log4j.appenders.compressedLogAggregationAppender.v1.HashMapKey.ByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v1.intermediateRepresentation.BufferedFileOutputStreamIR;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public abstract class BaseAggregationOutputStreamIR extends BufferedFileOutputStreamIR implements AggregationIR {
    ObjectOpenCustomHashSet<byte[]> deduplicationHashSet;   // logtype and variable type can share the same set

    static class Sha256DigestHashStrategy<K> implements Hash.Strategy<K> {
        @Override
        public int hashCode(Object o) {
            // Must be careful with sign extension when shifting
            // Leverage instruction level parallelism
            // Hashcode is simply the most significant 32 bytes
            byte[] digest = (byte[]) o;
            int a = (digest[0] & 0xFF) << 24;
            int b = (digest[1] & 0xFF) << 16;
            int c = (digest[2] & 0xFF) << 8;
            int d = digest[3] & 0xFF;
            int upper = a | b;
            int lower = c | d;
            return upper | lower;
        }

        @Override
        public boolean equals(Object a, Object b) {
            return Arrays.equals((byte[]) a, (byte[]) b);
        }
    }

    public BaseAggregationOutputStreamIR(Path path, int compressionLevel) throws IOException {
        super(path, compressionLevel);
        deduplicationHashSet = new ObjectOpenCustomHashSet<>(new Sha256DigestHashStrategy<>());
    }

    @Override
    public void putTimestamp(long timestamp) throws IOException {
        bufferedDataOutputStream.writeByte(0x10);
        bufferedDataOutputStream.writeLong(timestamp);
    }

    @Override
    public void putLogtype(ByteArrayViewDictionaryKey logtype) throws IOException, CloneNotSupportedException {
        byte[] digest = logtype.digest;
        if (deduplicationHashSet.contains(digest)) {
            bufferedDataOutputStream.writeByte(0x20);
            bufferedDataOutputStream.write(digest);
        } else {
            int viewSize = logtype.getViewSize();
            if (viewSize <= Byte.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x21);
                bufferedDataOutputStream.write(digest);
                bufferedDataOutputStream.writeByte(viewSize);
            } else if (viewSize <= Short.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x22);
                bufferedDataOutputStream.write(digest);
                bufferedDataOutputStream.writeShort(viewSize);
            } else {
                bufferedDataOutputStream.writeByte(0x23);
                bufferedDataOutputStream.write(digest);
                bufferedDataOutputStream.writeInt(viewSize);
            }
            bufferedDataOutputStream.write(logtype.bytes, logtype.beginIndex, viewSize);
            addDeduplicationHashSetEntry(digest);
        }
    }

    public void putDictionaryVariable(ByteArrayViewDictionaryKey variable) throws IOException {
        byte[] digest = variable.digest;
        if (deduplicationHashSet.contains(digest)) {
            bufferedDataOutputStream.writeByte(0x30);
            bufferedDataOutputStream.write(digest);
        } else {
            int viewSize = variable.getViewSize();
            if (viewSize <= Byte.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x31);
                bufferedDataOutputStream.write(digest);
                bufferedDataOutputStream.writeByte(viewSize);
            } else if (viewSize <= Short.MAX_VALUE) {
                bufferedDataOutputStream.writeByte(0x32);
                bufferedDataOutputStream.write(digest);
                bufferedDataOutputStream.writeShort(viewSize);
            } else {
                bufferedDataOutputStream.writeByte(0x33);
                bufferedDataOutputStream.write(digest);
                bufferedDataOutputStream.writeInt(viewSize);
            }
            bufferedDataOutputStream.write(variable.bytes, variable.beginIndex, viewSize);
            addDeduplicationHashSetEntry(digest);
        }
    }

    private void addDeduplicationHashSetEntry(byte[] digest) {
        // Insert a deep copy of digest into dictionary
        byte[] digestCopy = new byte[32];
        System.arraycopy(digest, 0, digestCopy, 0, 32);
        deduplicationHashSet.add(digestCopy);
    }

    public abstract void putEncodedVariable(ByteArrayViewDictionaryKey variable) throws IOException;
}
