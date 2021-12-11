package com.yscope.log4j.appenders.compressedLogAggregationAppender.v5.decompressionDictionary;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;

import java.io.IOException;
import java.util.Arrays;

public abstract class DecompressionDictionary {
    protected int nextDictionaryId = 0;
    Object2IntOpenCustomHashMap<byte[]> decompressionDictionary =
            new Object2IntOpenCustomHashMap<>(new ByteArrayHashStrategy<>());

    static class ByteArrayHashStrategy<K> implements Hash.Strategy<K> {
        @Override
        public int hashCode(Object o) {
            if (o instanceof byte[] byteArray) {
                return Arrays.hashCode(byteArray);
            }
            return 0;
        }

        @Override
        public boolean equals(Object a, Object b) {
            return Arrays.equals((byte[]) a, (byte[]) b);
        }
    }

    public abstract void close() throws IOException;

    protected abstract void persistDictionaryEntry(byte[] keyStr, int length) throws IOException;

    public int getExistingId(byte[] digest) throws IOException {
        return decompressionDictionary.getInt(digest);
    }

    public char getExistingCompactId(byte[] digest) throws IOException {
        int id = getExistingId(digest);
        if (id > Character.MAX_VALUE) {
            throw new StringIndexOutOfBoundsException("Dictionary key's length exceeds encoding capability");
        }
        return (char) id;
    }

    public int upsertId(byte[] digest, byte[] keyStr, int length) throws IOException {
        // Insert a more memory efficient byteArray copy instead of byteArrayView into the dictionary
        // This allows byteArrayView's underlying byte array to be garbage collected
        int id = decompressionDictionary.getOrDefault(digest, nextDictionaryId);
        if (id == nextDictionaryId) {
            byte[] digestCopy = new byte[32];
            System.arraycopy(digest, 0, digestCopy, 0, 32);
            decompressionDictionary.put(digestCopy, nextDictionaryId++);
            persistDictionaryEntry(keyStr, length);
        }
        return id;
    }

    public char upsertCompactId(byte[] digest, byte[] msg, int length) throws IOException {
        int id = upsertId(digest, msg, length);
        if (id > Character.MAX_VALUE) {
            throw new StringIndexOutOfBoundsException("Dictionary key's length exceeds encoding capability");
        }
        return (char) id;
    }


}
