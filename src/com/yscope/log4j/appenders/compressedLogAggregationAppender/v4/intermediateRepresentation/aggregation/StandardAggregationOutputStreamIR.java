package com.yscope.log4j.appenders.compressedLogAggregationAppender.v4.intermediateRepresentation.aggregation;

import com.yscope.log4j.appenders.compressedLogAggregationAppender.v4.HashMapKey.ByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v4.HashMapKey.StandardVariableByteArrayViewDictionaryKey;

import java.io.IOException;
import java.nio.file.Path;

public class StandardAggregationOutputStreamIR extends BaseAggregationOutputStreamIR {

    public StandardAggregationOutputStreamIR(Path path, int compressionLevel) throws IOException {
        super(path, compressionLevel);
    }

    @Override
    public void putEncodedVariable(ByteArrayViewDictionaryKey variable) throws IOException {
        bufferedDataOutputStream.writeByte(0x40);
        bufferedDataOutputStream.writeLong(
                ((StandardVariableByteArrayViewDictionaryKey)variable).getStandardVariableEncoding());
    }
}
