package com.yscope.log4j.appenders.compressedLogFileAppender.v3.utilityClasses;

import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.apache.logging.log4j.core.layout.ByteBufferDestinationHelper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class PatternLayoutBufferDestination implements ByteBufferDestination {
    public ByteBuffer buf;

    public PatternLayoutBufferDestination(int initialCapacity) {
         buf = ByteBuffer.allocate(initialCapacity);
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return buf;
    }

    @Override
    public ByteBuffer drain(ByteBuffer buf) {
        // Do nothing
        return buf.clear();
    }

    @Override
    public void writeBytes(ByteBuffer data) {
        synchronized (this) {
            if (data.remaining() > buf.capacity()) {
                // Pad buffer to multiple of 1024
                int multiple = data.remaining() / 1024;
                if (data.remaining() % 1024 > 0) {
                    multiple++;
                }
                buf = ByteBuffer.allocate(multiple * 1024);   // Increase by power of 2
            }
            buf.put(data);
        }
    }

    @Override
    public void writeBytes(byte[] data, int offset, int length) {
        if (data.length > buf.capacity()) {
            // Pad buffer to multiple of 1024
            int multiple = data.length / 1024;
            if (data.length % 1024 > 0) {
                multiple++;
            }
            buf = ByteBuffer.allocate(multiple * 1024);   // Increase by power of 2
        }
        buf.clear().flip();
        buf.put(data, offset, length);
        buf.flip();
    }


    @Override
    public String toString() {
        return new String(buf.array(), 0, buf.position(), StandardCharsets.UTF_8);
    }

    public String toString(ByteBuffer data) {
        data.mark();
        byte[] bytes = new byte[data.position()];
        data.get(bytes).rewind();
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
