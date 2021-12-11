package com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key;

/**
 * Lightweight dictionary sha256 digest entry
 * - fast hashcode and equality comparison
 * Warning: Only use Sha256DigestDictionaryEntry as dictionary entries.
 *          Other use-cases are not guaranteed to be correct
 */
public class Sha256DigestDictionaryKey implements Cloneable {
    public long a;   // bit 255-192
    public long b;   // bit 191-128
    public long c;   // bit 127-64
    public long d;   // bit 63-0

    public Sha256DigestDictionaryKey() {}

    public Sha256DigestDictionaryKey(long a, long b, long c, long d) {
        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
    }

    public void setDigest(byte[] bytes) {
        a = getLong(bytes, 0);
        b = getLong(bytes, 8);
        c = getLong(bytes, 16);
        d = getLong(bytes, 24);
    }

    private long getLong(byte[] bytes, int digestBufferOffset) {
        // Encode portion of digest as big endian long
        long output = ((long) bytes[digestBufferOffset] & 0xFF) << 56;
        output |= ((long) bytes[digestBufferOffset + 1] & 0xFF) << 48;
        output |= ((long) bytes[digestBufferOffset + 2] & 0xFF) << 40;
        output |= ((long) bytes[digestBufferOffset + 3] & 0xFF) << 32;
        output |= ((long) bytes[digestBufferOffset + 4] & 0xFF) << 24;
        output |= ((long) bytes[digestBufferOffset + 5] & 0xFF) << 16;
        output |= ((long) bytes[digestBufferOffset + 6] & 0xFF) << 8;
        output |= ((long) bytes[digestBufferOffset + 7] & 0xFF);
        return output;
    }

    @Override
    public int hashCode() {
        return (int) d;   // return bit 31-0
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Sha256DigestDictionaryKey otherDictionaryEntry &&
                this.a == otherDictionaryEntry.a && this.b == otherDictionaryEntry.b &&
                this.c == otherDictionaryEntry.c && this.d == otherDictionaryEntry.d;
    }

    @Override
    public String toString() {
        return Long.toHexString(a) + Long.toHexString(b) + Long.toHexString(c) + Long.toHexString(d);
    }

    public Sha256DigestDictionaryKey clone () throws CloneNotSupportedException {
        // Deep copy all primitive longs
        return (Sha256DigestDictionaryKey) super.clone();
    }
}
