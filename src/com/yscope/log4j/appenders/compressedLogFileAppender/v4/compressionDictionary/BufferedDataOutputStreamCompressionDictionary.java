package com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary;

import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key.ByteArrayViewDictionaryKey;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class BufferedDataOutputStreamCompressionDictionary extends CompressionDictionary {
    protected DataOutputStream bufferedDataOutputStream;

    public BufferedDataOutputStreamCompressionDictionary(final Path path, int compressionLevel)
            throws IOException {
        if (compressionLevel != 0) {
            BufferedOutputStream compressedInput = new BufferedOutputStream(Files.newOutputStream(path), 8192);
            ZstdCompressorOutputStream zstdCompressorOutputStream =
                    new ZstdCompressorOutputStream(compressedInput, compressionLevel);
            BufferedOutputStream decompressedInput = new BufferedOutputStream(zstdCompressorOutputStream, 16384);
            bufferedDataOutputStream = new DataOutputStream(decompressedInput);
        } else {
            bufferedDataOutputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }
    }

    @Override
    public void close() throws IOException {
        bufferedDataOutputStream.close();
    }

    @Override
    protected void persistDictionaryEntry(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey) {
        // Serialize and persist var dictionary with new id to memory mapped file
        // Format: <unsigned short length><utf8 string><unsigned short length><utf8 string>...
        // Java doesn't have unsigned short types except for char (16bit), therefore we cast it to char
        int dictionaryKeyLength = byteArrayViewDictionaryKey.getViewSize();
        if (dictionaryKeyLength > Character.MAX_VALUE) {
            throw new StringIndexOutOfBoundsException("Dictionary key's length exceeds encoding capability");
        }
        try {
            bufferedDataOutputStream.writeChar(dictionaryKeyLength);
            bufferedDataOutputStream.write(byteArrayViewDictionaryKey.bytes,
                    byteArrayViewDictionaryKey.beginIndex, dictionaryKeyLength);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
