package com.yscope.log4j.appenders.compressedLogFileAppender.v3.compressionDictionary;

import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.ByteArrayView;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.CompactVariableByteArrayView;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.VariableByteArrayView;

import java.nio.MappedByteBuffer;

public class VariableCompressionDictionary extends CompressionDictionary {
    public VariableCompressionDictionary(MappedByteBuffer dictBuf) {
        super(dictBuf);
    }

    public int getVariableId(ByteArrayView variableWrapper) {
        return getId(variableWrapper);
    }

}
