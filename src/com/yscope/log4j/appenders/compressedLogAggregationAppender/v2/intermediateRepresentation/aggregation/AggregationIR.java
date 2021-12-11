package com.yscope.log4j.appenders.compressedLogAggregationAppender.v2.intermediateRepresentation.aggregation;


import com.yscope.log4j.appenders.compressedLogAggregationAppender.v2.HashMapKey.ByteArrayViewDictionaryKey;

import java.io.IOException;

public interface AggregationIR {
    void putTimestamp(long timestamp) throws IOException;
    void putLogtype(ByteArrayViewDictionaryKey logtype) throws IOException;
    void putEncodedVariable(ByteArrayViewDictionaryKey variable) throws IOException;
    void putDictionaryVariable(ByteArrayViewDictionaryKey variable) throws IOException;
    void close() throws IOException;
}
