package com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary;

import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key.ByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key.Sha256DigestDictionaryKey;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.IOException;

public abstract class CompressionDictionary {
    protected  int nextDictionaryId;
    protected  Object2IntOpenHashMap<Sha256DigestDictionaryKey> compressionDictionary = new Object2IntOpenHashMap<>();

    public abstract void close() throws IOException;

    protected abstract void persistDictionaryEntry(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey) throws IOException;

    public int getId(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey) throws IOException, CloneNotSupportedException {
        Sha256DigestDictionaryKey sha256DigestDictionaryKey = byteArrayViewDictionaryKey.getSha256DigestDictionaryKey();
        int id = compressionDictionary.getOrDefault(sha256DigestDictionaryKey, nextDictionaryId);
        if (id == nextDictionaryId) {
            // Insert a more memory efficient sha256 dictionary key into the dictionary
            compressionDictionary.put(sha256DigestDictionaryKey.clone(), nextDictionaryId++);
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
