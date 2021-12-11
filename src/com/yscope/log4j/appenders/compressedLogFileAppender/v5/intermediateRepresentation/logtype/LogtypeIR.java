package com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation.logtype;

import java.io.IOException;

public interface LogtypeIR {
    void putLogtype(long logtype) throws IOException;
    void close() throws IOException;
}
