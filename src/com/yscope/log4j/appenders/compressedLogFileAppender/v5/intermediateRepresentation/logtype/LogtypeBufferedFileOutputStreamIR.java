package com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.logtype;

import com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.BufferedFileOutputStreamIR;

import java.io.IOException;
import java.nio.file.Path;

public class LogtypeBufferedFileOutputStreamIR extends BufferedFileOutputStreamIR implements LogtypeIR {
    public LogtypeBufferedFileOutputStreamIR(Path path, Integer compressionLevel) throws IOException {
        super(path, compressionLevel);
    }

    @Override
    public void putLogtype(long logtype) throws IOException {
        putLong(logtype);
    }
}
