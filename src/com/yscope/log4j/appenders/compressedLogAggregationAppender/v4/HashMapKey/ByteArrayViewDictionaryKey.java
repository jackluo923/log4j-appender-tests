package com.yscope.log4j.appenders.compressedLogAggregationAppender.v4.HashMapKey;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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

    // Byte array view digest
    public MessageDigest messageDigestInstance;
    public byte[] digest;   // Must be exactly 32 bytes


    public ByteArrayViewDictionaryKey() throws NoSuchAlgorithmException {
        this.messageDigestInstance = MessageDigest.getInstance("SHA-256");
        this.digest = new byte[32];
    }

    public ByteArrayViewDictionaryKey(ByteBuffer heapAllocatedBuffer, int beginIndex, int endIndex)
            throws NoSuchAlgorithmException, DigestException {
        this();
        wrap(heapAllocatedBuffer, beginIndex, endIndex);
    }

    public ByteArrayViewDictionaryKey(byte[] bytes, int beginIndex, int endIndex)
            throws NoSuchAlgorithmException, DigestException {
        this();
        wrap(bytes, beginIndex, endIndex);
    }

    public ByteArrayViewDictionaryKey wrap(ByteBuffer heapAllocatedByteBuffer) throws DigestException {
        if (heapAllocatedByteBuffer.isDirect()) {
            throw new UnsupportedOperationException("Does not support direct byte buffer");
        }
        wrap(heapAllocatedByteBuffer.array(), 0, heapAllocatedByteBuffer.position());
        return this;
    }

    public ByteArrayViewDictionaryKey wrap(ByteBuffer heapAllocatedByteBuffer, int beginIndex, int endIndex)
            throws DigestException {
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

    public ByteArrayViewDictionaryKey wrap(byte[] bytes, int beginIndex, int endIndex) throws DigestException {
        this.bytes = bytes;
        this.beginIndex = beginIndex;
        this.endIndex = endIndex;

        // Since view changed, we need to recompute the digest and hash code
        messageDigestInstance.update(bytes, beginIndex, endIndex);
        messageDigestInstance.digest(digest, 0, 32);
        return this;
    }

    public int getViewSize() {
        return endIndex - beginIndex;
    }

    @Override
    public String toString() {
        return new String(bytes, beginIndex, getViewSize(), StandardCharsets.UTF_8);
    }
}
