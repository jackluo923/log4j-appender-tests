package com.yscope.log4j.appenders.compressedLogAggregationAppender.v4.intermediateRepresentation.aggregation;

import com.yscope.log4j.appenders.compressedLogAggregationAppender.v4.HashMapKey.ByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v4.HashMapKey.CompactVariableByteArrayViewDictionaryKey;

import java.io.IOException;
import java.nio.file.Path;

public class CompactAggregationOutputStreamIR extends BaseAggregationOutputStreamIR {

    public CompactAggregationOutputStreamIR(Path path, int compressionLevel) throws IOException {
        super(path, compressionLevel);
    }

    @Override
    public void putEncodedVariable(ByteArrayViewDictionaryKey variable) throws IOException {
        bufferedDataOutputStream.writeByte(0x50);
        bufferedDataOutputStream.writeInt(
                ((CompactVariableByteArrayViewDictionaryKey) variable).getCompactVariableEncoding());
    }
}
