package com.yscope.log4j.appenders.compressedLogFileAppender.v2.compressionDictionary;

import com.yscope.log4j.appenders.compressedLogFileAppender.v2.binaryDataWrappers.ByteArray;
import com.yscope.log4j.appenders.compressedLogFileAppender.v2.binaryDataWrappers.ByteArrayView;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.nio.MappedByteBuffer;

public class CompressionDictionary {
    protected  int nextDictionaryId;
    protected  Object2IntOpenHashMap<ByteArray> compressionDictionary = new Object2IntOpenHashMap<>();
    protected  final MappedByteBuffer compressionDictBuf;

    public CompressionDictionary(MappedByteBuffer dictBuf) {
        this.compressionDictBuf = dictBuf;
    }

    public int getId(ByteArrayView byteArrayView) {
        int id = compressionDictionary.getOrDefault(byteArrayView, nextDictionaryId);
        if (id == nextDictionaryId) {
            // Insert a more memory efficient byteArray copy instead of byteArrayView into the dictionary
            // This allows byteArrayView's underlying byte array to be garbage collected
            ByteArray dictionaryKey = new ByteArray(byteArrayView.getByteArrayViewRegionBytesDeepCopy());
            compressionDictionary.put(dictionaryKey, nextDictionaryId++);

            // Serialize and persist var dictionary with new id to memory mapped file
            // Format: <unsigned short length><utf8 string><unsigned short length><utf8 string>...
            // Java doesn't have unsigned short types except for char (16bit), therefore we cast it to char
            int viewSize = dictionaryKey.getViewSize();
            if (viewSize > Character.MAX_VALUE) {
                throw new StringIndexOutOfBoundsException("Dictionary key's length exceeds encoding capability");
            }
            compressionDictBuf.putChar((char) viewSize);
            compressionDictBuf.put(dictionaryKey.bytes);
        }
        return id;
    }
}
