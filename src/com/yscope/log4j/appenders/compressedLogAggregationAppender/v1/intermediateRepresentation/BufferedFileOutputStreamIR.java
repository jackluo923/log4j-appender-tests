package com.yscope.log4j.appenders.compressedLogAggregationAppender.v1.intermediateRepresentation;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class BufferedFileOutputStreamIR implements IR {
    public DataOutputStream bufferedDataOutputStream;

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
}
