package com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.timestamp;

import java.io.IOException;

public interface TimestampIR {
    void putTimestamp(long timestamp) throws IOException;
    void close() throws IOException;
}
