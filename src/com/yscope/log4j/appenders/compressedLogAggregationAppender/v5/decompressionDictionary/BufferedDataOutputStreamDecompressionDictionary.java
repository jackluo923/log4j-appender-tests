package com.yscope.log4j.appenders.compressedLogAggregationAppender.v5.decompressionDictionary;

import com.yscope.log4j.appenders.compressedLogAggregationAppender.v1.decompressionDictionary.DecompressionDictionary;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class BufferedDataOutputStreamDecompressionDictionary extends DecompressionDictionary {
    protected DataOutputStream bufferedDataOutputStream;

    public BufferedDataOutputStreamDecompressionDictionary(final Path path, int compressionLevel)
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
    protected void persistDictionaryEntry(byte[] keyStr, int length) throws IOException {
        // Serialize and persist var dictionary with new id to memory mapped file
        // Format: <unsigned short length><utf8 string><unsigned short length><utf8 string>...
        // Java doesn't have unsigned short types except for char (16bit), therefore we cast it to char
        if (length > Character.MAX_VALUE) {
            throw new StringIndexOutOfBoundsException("Dictionary key's length exceeds encoding capability");
        }
        bufferedDataOutputStream.writeChar(length);
        bufferedDataOutputStream.write(keyStr, 0, length);
    }
}
