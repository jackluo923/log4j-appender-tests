package com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.timestamp;

import com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.MemoryMappedIR;

import java.io.IOException;
import java.nio.file.Path;

public class TimestampMemoryMappedIR extends MemoryMappedIR implements TimestampIR {
    public TimestampMemoryMappedIR(Path path, int maxBufSize, Integer compressionLevel) throws IOException {
        super(path, maxBufSize, compressionLevel);
    }

    @Override
    public void putTimestamp(long timestamp) throws IOException {
        putLong(timestamp);
    }
}
