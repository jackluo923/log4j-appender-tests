package com.yscope.log4j.appenders.compressedLogFileAppender.v3.compressionDictionary;

import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.LogtypeByteArrayView;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.nio.MappedByteBuffer;

public class BufferedLogtypeCompressionDictionary extends BufferedCompressionDictionary {
    public BufferedLogtypeCompressionDictionary(DataOutputStream compressionDictBDOS) {
        super(compressionDictBDOS);
    }

    public int getLogtypeId(LogtypeByteArrayView logtypeByteBuffer) {
        return getId(logtypeByteBuffer);
    }

    // Return unsigned integer representation
    public char getLogtypeIdCompact(LogtypeByteArrayView logtypeByteBuffer) {
        return (char) getId(logtypeByteBuffer);
    }
}
