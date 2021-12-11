package com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.variable;

import com.yscope.log4j.appenders.compressedLogFileAppender.v5.compressionDictionary.CompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v5.compressionDictionary.key.CompactVariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogFileAppender.v5.compressionDictionary.key.VariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.MemoryMappedIR;

import java.io.IOException;
import java.nio.file.Path;

public class CompactVariableMemoryMappedIR extends MemoryMappedIR implements VariableIR {
    public CompactVariableMemoryMappedIR(final Path path, int maxBufSize, Integer compressionLevel) throws IOException {
        super(path, maxBufSize, compressionLevel);
    }

    @Override
    public void putDecimalEncodedVariable(VariableByteArrayViewDictionaryKey variableByteArrayViewDictionaryKey)
            throws IOException {
        putInt(((CompactVariableByteArrayViewDictionaryKey)variableByteArrayViewDictionaryKey).
                getCompactVariableEncoding());
    }

    @Override
    public void putIntegerEncodedVariable(VariableByteArrayViewDictionaryKey variableByteArrayViewDictionaryKey)
            throws IOException {
        putInt(((CompactVariableByteArrayViewDictionaryKey)variableByteArrayViewDictionaryKey).
                getCompactVariableEncoding());
    }

    @Override
    public void putVariableID(VariableByteArrayViewDictionaryKey variableByteArrayViewDictionaryKey,
                              CompressionDictionary variableDict) throws IOException, CloneNotSupportedException {
        putInt(variableDict.getCompactId(variableByteArrayViewDictionaryKey));
    }
}
