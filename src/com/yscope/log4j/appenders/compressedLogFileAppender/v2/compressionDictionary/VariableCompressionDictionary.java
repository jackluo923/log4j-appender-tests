package com.yscope.log4j.appenders.compressedLogFileAppender.v2.compressionDictionary;

import com.yscope.log4j.appenders.compressedLogFileAppender.v2.binaryDataWrappers.VariableByteArrayView;

import java.nio.MappedByteBuffer;

public class VariableCompressionDictionary extends CompressionDictionary {
    public VariableCompressionDictionary(MappedByteBuffer dictBuf) {
        super(dictBuf);
    }

    public int getVariableId(VariableByteArrayView variableWrapper) {
        return getId(variableWrapper);
    }
}
