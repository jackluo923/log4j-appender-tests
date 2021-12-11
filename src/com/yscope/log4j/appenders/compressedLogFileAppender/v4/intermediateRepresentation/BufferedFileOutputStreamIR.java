package com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class BufferedFileOutputStreamIR implements IR {
    protected DataOutputStream bufferedDataOutputStream;

    public BufferedFileOutputStreamIR(final Path path, int compressionLevel) throws IOException {
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

    public void close() throws IOException {
        bufferedDataOutputStream.close();
    }

    @Override
    public void putByte(byte val) throws IOException {
        bufferedDataOutputStream.writeByte(val);
    }

    @Override
    public void putChar(char val) throws IOException {
        bufferedDataOutputStream.writeChar(val);
    }

    @Override
    public void putInt(int val) throws IOException {
        bufferedDataOutputStream.writeInt(val);
    }

    @Override
    public void putLong(long val) throws IOException {
        bufferedDataOutputStream.writeLong(val);
    }
}
