package com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.variable;

import com.yscope.log4j.appenders.compressedLogFileAppender.v5.compressionDictionary.CompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v5.compressionDictionary.key.VariableByteArrayViewDictionaryKey;

import java.io.IOException;

public interface VariableIR {
    void putDecimalEncodedVariable(VariableByteArrayViewDictionaryKey variableByteArrayViewDictionaryKey)
            throws IOException;
    void putIntegerEncodedVariable(VariableByteArrayViewDictionaryKey variableByteArrayViewDictionaryKey)
            throws IOException;
    void putVariableID(VariableByteArrayViewDictionaryKey variableByteArrayViewDictionaryKey,
                       CompressionDictionary variableDict) throws IOException, CloneNotSupportedException;
    void close() throws IOException;
}
