package com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key;

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
    public byte[] bytes;
    public int beginIndex;
    public int endIndex;
    private final MessageDigest messageDigestInstance;
    private final byte[] digestBuffer;
    private final Sha256DigestDictionaryKey sha256DigestDictionaryKey;

    public ByteArrayViewDictionaryKey() throws NoSuchAlgorithmException {
        this.messageDigestInstance = MessageDigest.getInstance("SHA-256");
        this.digestBuffer = new byte[this.messageDigestInstance.getDigestLength()];
        this.sha256DigestDictionaryKey = new Sha256DigestDictionaryKey();
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

    public ByteArrayViewDictionaryKey wrap(ByteBuffer heapAllocatedByteBuffer, int beginIndex, int endIndex) throws DigestException {
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
        messageDigestInstance.digest(digestBuffer, 0, digestBuffer.length);   // Store output in digestBuffer

        // Store digest as 4 signed long
        sha256DigestDictionaryKey.setDigest(digestBuffer);
        return this;
    }

    // Obtain a minimal dictionary key specifically designed to server as hashmap key
    public Sha256DigestDictionaryKey getSha256DigestDictionaryKey() {
        return sha256DigestDictionaryKey;
    }



    public int getViewSize() {
        return endIndex - beginIndex;
    }

    @Override
    public String toString() {
        return new String(bytes, beginIndex, getViewSize(), StandardCharsets.UTF_8);
    }
}
