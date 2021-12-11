package com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.logtype;

import java.io.IOException;

public interface LogtypeIR {
    void putLogtype(long logtype) throws IOException;
    void close() throws IOException;
}
