package com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.intermediateRepresentation;

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
            bufferedDataOutputStream =
                    new DataOutputStream(new ZstdCompressorOutputStream(Files.newOutputStream(path), compressionLevel));
        } else {
            bufferedDataOutputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }
    }

    public void close() throws IOException {
        bufferedDataOutputStream.close();
    }
}
