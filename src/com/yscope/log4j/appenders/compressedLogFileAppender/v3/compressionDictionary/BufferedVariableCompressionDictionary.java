package com.yscope.log4j.appenders.compressedLogFileAppender.v3.compressionDictionary;

import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.ByteArrayView;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.nio.MappedByteBuffer;

public class BufferedVariableCompressionDictionary extends BufferedCompressionDictionary {
    public BufferedVariableCompressionDictionary(DataOutputStream compressionDictBOS) {
        super(compressionDictBOS);
    }

    public int getVariableId(ByteArrayView variableWrapper) {
        return getId(variableWrapper);
    }

}
