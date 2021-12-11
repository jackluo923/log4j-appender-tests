package com.yscope.log4j.appenders.compressedLogAggregationAppender.v2.intermediateRepresentation.aggregation;

import com.yscope.log4j.appenders.compressedLogAggregationAppender.v2.HashMapKey.ByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v2.intermediateRepresentation.BufferedFileOutputStreamIR;

import java.io.IOException;
import java.nio.file.Path;

public abstract class BaseAggregationOutputStreamIR extends BufferedFileOutputStreamIR implements AggregationIR {
    public BaseAggregationOutputStreamIR(Path path, int compressionLevel) throws IOException {
        super(path, compressionLevel);
    }

    @Override
    public void putTimestamp(long timestamp) throws IOException {
        bufferedDataOutputStream.writeByte(0x10);
        bufferedDataOutputStream.writeLong(timestamp);
    }

    @Override
    public void putLogtype(ByteArrayViewDictionaryKey logtype) throws IOException {
        int viewSize = logtype.getViewSize();
        if (viewSize <= Byte.MAX_VALUE) {
            bufferedDataOutputStream.writeByte(0x21);
            bufferedDataOutputStream.writeByte(viewSize);
        } else if (viewSize <= Short.MAX_VALUE) {
            bufferedDataOutputStream.writeByte(0x22);
            bufferedDataOutputStream.writeShort(viewSize);

        } else {
            bufferedDataOutputStream.writeByte(0x23);
            bufferedDataOutputStream.writeInt(viewSize);
        }
        bufferedDataOutputStream.write(logtype.bytes, logtype.beginIndex, logtype.endIndex);
    }

    public void putDictionaryVariable(ByteArrayViewDictionaryKey variable) throws IOException {
        int viewSize = variable.getViewSize();
        if (viewSize <= Byte.MAX_VALUE) {
            bufferedDataOutputStream.writeByte(0x31);
            bufferedDataOutputStream.writeByte(viewSize);
        } else if (viewSize <= Short.MAX_VALUE) {
            bufferedDataOutputStream.writeByte(0x32);
            bufferedDataOutputStream.writeShort(viewSize);
        } else {
            bufferedDataOutputStream.writeByte(0x33);
            bufferedDataOutputStream.writeInt(viewSize);
        }
        bufferedDataOutputStream.write(variable.bytes, variable.beginIndex, viewSize);
    }

    public abstract void putEncodedVariable(ByteArrayViewDictionaryKey variable) throws IOException;
}
