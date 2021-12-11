package com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.HashMapKey;

import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.ByteArrayView;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.DigestException;
import java.util.Arrays;

/**
 * This class should be instantiated preferably once, and wrap byte array and range afterwards
 * Warning: Only use ByteArrayViewDictionaryEntry as dictionary entries.
 *          Other use-case are not guaranteed to be correct
 */
public class ByteArrayViewDictionaryKey {
    // Byte array view
    public byte[] bytes;
    public int beginIndex;
    public int endIndex;

    public ByteArrayViewDictionaryKey() {}

    public ByteArrayViewDictionaryKey(ByteBuffer heapAllocatedBuffer, int beginIndex, int endIndex) {
        this();
        wrap(heapAllocatedBuffer, beginIndex, endIndex);
    }

    public ByteArrayViewDictionaryKey(byte[] bytes, int beginIndex, int endIndex) {
        this();
        wrap(bytes, beginIndex, endIndex);
    }

    public ByteArrayViewDictionaryKey wrap(ByteBuffer heapAllocatedByteBuffer) {
        if (heapAllocatedByteBuffer.isDirect()) {
            throw new UnsupportedOperationException("Does not support direct byte buffer");
        }
        wrap(heapAllocatedByteBuffer.array(), 0, heapAllocatedByteBuffer.position());
        return this;
    }

    public ByteArrayViewDictionaryKey wrap(ByteBuffer heapAllocatedByteBuffer, int beginIndex, int endIndex) {
        if (heapAllocatedByteBuffer.isDirect()) {
            throw new UnsupportedOperationException("Does not support direct byte buffer");
        }
        wrap(heapAllocatedByteBuffer.array(), beginIndex, endIndex);
        return this;
    }

    public ByteArrayViewDictionaryKey wrap(byte[] bytes) throws DigestException {
        wrap(bytes, 0, bytes.length);
        return this;
    }

    public ByteArrayViewDictionaryKey wrap(byte[] bytes, int beginIndex, int endIndex){
        this.bytes = bytes;
        this.beginIndex = beginIndex;
        this.endIndex = endIndex;
        return this;
    }

    public int getViewSize() {
        return endIndex - beginIndex;
    }

    @Override
    public String toString() {
        return new String(bytes, beginIndex, getViewSize(), StandardCharsets.UTF_8);
    }

    @Override
    public int hashCode() {
        // Adapted from JDK's Arrays.hashCode() method
        // Compute the hashcode for the byteArray view only
        int result = 1;
        for (int i = beginIndex; i < endIndex; i++) {
            result = 31 * result + bytes[i];
        }
        return result;
    }

    @Override
    public boolean equals(Object other) {
        // This equal function will primarily be comparing byteArrays
        if (other instanceof byte[] otherByteArray) {
            return Arrays.equals(bytes, beginIndex, endIndex, otherByteArray, 0, otherByteArray.length);
        } else if (other instanceof ByteArrayView otherByteArrayView) {
            return Arrays.equals(bytes, beginIndex, endIndex,
                    otherByteArrayView.bytes, otherByteArrayView.beginIndex, otherByteArrayView.endIndex);
        }
        return false;
    }
}
