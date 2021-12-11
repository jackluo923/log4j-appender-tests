package com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.decompressionDictionary;

import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.HashMapKey.ByteArrayViewDictionaryKey;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;

import java.io.IOException;
import java.util.Arrays;

public abstract class DecompressionDictionary {
    protected int nextDictionaryId = 0;
    Object2IntOpenCustomHashMap<Object> decompressionDictionary =
            new Object2IntOpenCustomHashMap<>(new ByteArrayHashStrategy<>());

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

    public abstract void close() throws IOException;

    protected abstract void persistDictionaryEntry(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey);

    public int getExistingId(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey) throws IOException {
        return decompressionDictionary.getInt(byteArrayViewDictionaryKey);
    }

    public int getExistingCompactId(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey) throws IOException {
        int id = getExistingId(byteArrayViewDictionaryKey);
        if (id > Character.MAX_VALUE) {
            throw new StringIndexOutOfBoundsException("Dictionary key's length exceeds encoding capability");
        }
        return (char) id;
    }

    public int upsertId(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey) throws IOException {
        int id = decompressionDictionary.getOrDefault(byteArrayViewDictionaryKey, nextDictionaryId);
        if (id == nextDictionaryId) {
            // Insert a more memory efficient byteArray copy instead of byteArrayView into the dictionary
            // This allows byteArrayView's underlying byte array to be garbage collected
            int viewSize = byteArrayViewDictionaryKey.getViewSize();
            byte[] keyCopy = new byte[viewSize];
            System.arraycopy(byteArrayViewDictionaryKey.bytes, byteArrayViewDictionaryKey.beginIndex,
                    keyCopy, 0, viewSize);
            decompressionDictionary.put(keyCopy, nextDictionaryId++);
            persistDictionaryEntry(byteArrayViewDictionaryKey);
        }
        return id;
    }

    public char upsertCompactId(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey) throws IOException {
        int id = upsertId(byteArrayViewDictionaryKey);
        if (id > Character.MAX_VALUE) {
            throw new StringIndexOutOfBoundsException("Dictionary key's length exceeds encoding capability");
        }
        return (char) id;
    }
}
