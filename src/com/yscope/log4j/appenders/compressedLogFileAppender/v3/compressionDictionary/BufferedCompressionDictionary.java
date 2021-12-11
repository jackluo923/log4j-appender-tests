package com.yscope.log4j.appenders.compressedLogFileAppender.v3.compressionDictionary;

import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.ByteArray;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.ByteArrayView;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.DataOutputStream;
import java.io.IOException;

public class BufferedCompressionDictionary {
    protected int nextDictionaryId;
    protected Object2IntOpenHashMap<ByteArray> compressionDictionary = new Object2IntOpenHashMap<>();
    protected final DataOutputStream compressionDictBDOS;

    public BufferedCompressionDictionary(DataOutputStream compressionDictBDOS) {
        this.compressionDictBDOS = compressionDictBDOS;
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
            try {
                compressionDictBDOS.write((char) viewSize);
                compressionDictBDOS.write(dictionaryKey.bytes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return id;
    }

}
