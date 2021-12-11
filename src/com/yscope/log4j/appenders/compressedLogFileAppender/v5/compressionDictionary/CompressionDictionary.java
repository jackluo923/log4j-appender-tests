package com.yscope.log4j.appenders.compressedLogFileAppender.v5.compressionDictionary;

import com.yscope.log4j.appenders.compressedLogFileAppender.v5.compressionDictionary.key.ByteArrayViewDictionaryKey;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;

import java.io.IOException;

public abstract class CompressionDictionary {
    protected int nextDictionaryId = 0;
    protected Object2IntOpenCustomHashMap<byte[]> compressionDictionary =
            new Object2IntOpenCustomHashMap<>(new Sha256DigestHashStrategy<>());

    public abstract void close() throws IOException;

    protected abstract void persistDictionaryEntry(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey)
            throws IOException;

    public int getId(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey) throws IOException {
        final byte[] digest = byteArrayViewDictionaryKey.digest;
        int id = compressionDictionary.getOrDefault(digest, nextDictionaryId);
        if (id == nextDictionaryId) {
            // Insert a deep copy of sha256 digest into the dictionary
            byte[] digestCopy = new byte[32];
            System.arraycopy(digest, 0, digestCopy, 0, 32);
            compressionDictionary.put(digestCopy, nextDictionaryId++);
            persistDictionaryEntry(byteArrayViewDictionaryKey);
        }
        return id;
    }

    public char getCompactId(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey)
            throws IOException, CloneNotSupportedException {
        int id = getId(byteArrayViewDictionaryKey);
        if (id > Character.MAX_VALUE) {
            throw new StringIndexOutOfBoundsException("Dictionary key's length exceeds encoding capability");
        }
        return (char) id;
    }
}
