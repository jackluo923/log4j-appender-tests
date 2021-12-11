package com.yscope.log4j.appenders.compressedLogAggregationAppender.v1.utilityClasses;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class DecompressionDictionaryEntry {
    private final ByteBuffer directByteBufferReference;
    private final int beginIndex;
    public final int length;

    public DecompressionDictionaryEntry(ByteBuffer directByteBufferReference, int beginIndex, int length) {
        this.directByteBufferReference = directByteBufferReference;
        this.beginIndex = beginIndex;
        this.length = length;
    }

    public byte getByte(int index) {
        return directByteBufferReference.get(beginIndex + index);
    }

    public byte[] getBytes() {
        byte[] bytes = new byte[length];
        directByteBufferReference.position(beginIndex);
        directByteBufferReference.get(bytes);
        return bytes;
    }

    @Override
    public String toString() {
        return new String(getBytes(), StandardCharsets.UTF_8);
    }
}
