package com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation;

import java.io.IOException;

public interface IR {
    void close() throws IOException;
    void putByte(byte val) throws IOException;
    void putChar(char val) throws IOException;
    void putInt(int val) throws IOException;
    void putLong(long val) throws IOException;
}
