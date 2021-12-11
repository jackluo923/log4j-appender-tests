package com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.variable;

import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.CompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key.StandardVariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key.VariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.BufferedFileOutputStreamIR;

import java.io.IOException;
import java.nio.file.Path;

public class StandardVariableBufferedFileOutputStreamIR extends BufferedFileOutputStreamIR implements VariableIR {
    public StandardVariableBufferedFileOutputStreamIR(Path path, Integer compressionLevel) throws IOException {
        super(path, compressionLevel);
    }

    @Override
    public void putDecimalEncodedVariable(VariableByteArrayViewDictionaryKey variableByteArrayViewDictionaryKey)
            throws IOException {
        putLong(((StandardVariableByteArrayViewDictionaryKey)variableByteArrayViewDictionaryKey).
                getStandardVariableEncoding());
    }

    @Override
    public void putIntegerEncodedVariable(VariableByteArrayViewDictionaryKey variableByteArrayViewDictionaryKey)
            throws IOException {
        putLong(((StandardVariableByteArrayViewDictionaryKey)variableByteArrayViewDictionaryKey).
                getStandardVariableEncoding());
    }

    @Override
    public void putVariableID(VariableByteArrayViewDictionaryKey variableByteArrayViewDictionaryKey,
                              CompressionDictionary variableDict) throws IOException, CloneNotSupportedException {
        putLong(variableDict.getId(variableByteArrayViewDictionaryKey));
    }
}
