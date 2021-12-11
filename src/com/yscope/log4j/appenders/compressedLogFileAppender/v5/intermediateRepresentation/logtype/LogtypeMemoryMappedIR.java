package com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.logtype;

import com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.MemoryMappedIR;

import java.io.IOException;
import java.nio.file.Path;

public class LogtypeMemoryMappedIR extends MemoryMappedIR implements LogtypeIR {

    public LogtypeMemoryMappedIR(Path path, int maxBufSize, Integer compressionLevel) throws IOException {
        super(path, maxBufSize, compressionLevel);
    }

    @Override
    public void putLogtype(long logtype) throws IOException {
        putLong(logtype);
    }
}
