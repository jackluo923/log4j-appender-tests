package com.yscope.log4j.appenders.compressedLogAggregationAppender.v5.HashMapKey;

import org.apache.commons.codec.binary.Hex;

/**
 * Lightweight dictionary sha256 digest entry
 * - fast hashcode and equality comparison
 * Warning: Only use Sha256DigestDictionaryEntry as dictionary entries.
 *          Other use-cases are not guaranteed to be correct
 */
public class Sha256DigestDictionaryKey implements Cloneable {
    public byte[] digest;   // Must be exactly 32 bytes

    private Sha256DigestDictionaryKey(byte[] digest) {
        this.digest = digest;
    }

    public Sha256DigestDictionaryKey wrap(byte[] digest) {
        return new Sha256DigestDictionaryKey(digest);
    }

    @Override
    public String toString() {
        return Hex.encodeHexString(digest);
    }

    public Sha256DigestDictionaryKey getDeepCopy() {
        byte[] copy = new byte[32];
        System.arraycopy(digest, 0, copy, 0, 32);
        return new Sha256DigestDictionaryKey(copy);
    }
}
