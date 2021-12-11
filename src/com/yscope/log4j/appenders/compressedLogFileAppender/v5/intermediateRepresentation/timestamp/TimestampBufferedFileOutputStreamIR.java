package com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.timestamp;

import com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.BufferedFileOutputStreamIR;

import java.io.IOException;
import java.nio.file.Path;

public class TimestampBufferedFileOutputStreamIR extends BufferedFileOutputStreamIR implements TimestampIR {
    public TimestampBufferedFileOutputStreamIR(Path path, int compressionLevel) throws IOException {
        super(path, compressionLevel);
    }

    @Override
    public void putTimestamp(long timestamp) throws IOException {
        putLong(timestamp);
    }
}
