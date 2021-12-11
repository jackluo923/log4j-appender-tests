package com.yscope.log4j.appenders.compressedLogFileAppender.v2.compressionDictionary;

import com.yscope.log4j.appenders.compressedLogFileAppender.v2.binaryDataWrappers.LogtypeByteArrayView;

import java.nio.MappedByteBuffer;

public class LogtypeCompressionDictionary extends CompressionDictionary {
    public LogtypeCompressionDictionary(MappedByteBuffer dictBuf) {
        super(dictBuf);
    }

    public int getLogtypeId(LogtypeByteArrayView logtypeByteBuffer) {
        return getId(logtypeByteBuffer);
    }

    // Return unsigned integer representation
    public char getLogtypeIdCompact(LogtypeByteArrayView logtypeByteBuffer) {
        return (char) getId(logtypeByteBuffer);
    }
}
