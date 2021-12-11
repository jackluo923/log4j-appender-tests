package com.yscope.log4j.appenders.compressedLogAggregationAppender.v4.intermediateRepresentation.aggregation;

import com.yscope.log4j.appenders.compressedLogAggregationAppender.v4.HashMapKey.ByteArrayViewDictionaryKey;

import java.io.IOException;

public interface AggregationIR {
    void putTimestamp(long timestamp) throws IOException;
    void putLogtype(ByteArrayViewDictionaryKey logtype) throws IOException, CloneNotSupportedException;
    void putEncodedVariable(ByteArrayViewDictionaryKey variable) throws IOException;
    void putDictionaryVariable(ByteArrayViewDictionaryKey variable) throws IOException, CloneNotSupportedException;
    void close() throws IOException;
}
