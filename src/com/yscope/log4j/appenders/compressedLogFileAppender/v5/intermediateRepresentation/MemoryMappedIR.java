package com.yscope.log4j.appenders.compressedLogFileAppender.v5.intermediateRepresentation;

import com.github.luben.zstd.ZstdDirectBufferCompressingStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public abstract class MemoryMappedIR implements IR {
    private final FileChannel fc;
    private final ByteBuffer encodedDataBuffer;
    private final MappedByteBuffer memoryMappedBuffer;
    private ZstdDirectBufferCompressingStream zstdDirectBufferCompressingStream = null;

    public MemoryMappedIR(final Path path, int maxBufSize, int compressionLevel) throws IOException {
        fc = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        memoryMappedBuffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, maxBufSize);
        if (compressionLevel != 0) {
            encodedDataBuffer = memoryMappedBuffer;
        } else {
            encodedDataBuffer = ByteBuffer.allocateDirect(256);   // Allocate 256 native buffer for now
            zstdDirectBufferCompressingStream =
                    new ZstdDirectBufferCompressingStream(memoryMappedBuffer, compressionLevel);
        }
    }

    public void close() throws IOException {
        if (zstdDirectBufferCompressingStream != null) {
            // Flush encoded data cache to zstd, compress and close
            zstdDirectBufferCompressingStream.compress(encodedDataBuffer);
            zstdDirectBufferCompressingStream.close();
        }
        // Close memory mapped file buffer
        final int position = memoryMappedBuffer.position();
        // TODO: It's possible this call may fail because the buffer is not unmapped. In that case,
        // we may need to call System.gc() or use a more advanced method of forcing it to be unmapped.
        fc.truncate(position);
        fc.close();
    }

    @Override
    public void putByte(byte val) throws IOException {
        if (encodedDataBuffer.remaining() < Byte.SIZE & zstdDirectBufferCompressingStream != null) {
            // Need to flush buffer to zstd if encoded buffer data is full
            zstdDirectBufferCompressingStream.compress(encodedDataBuffer);
        }
        encodedDataBuffer.put(val);
    }

    @Override
    public void putChar(char val) throws IOException {
        if (encodedDataBuffer.remaining() < Character.SIZE & zstdDirectBufferCompressingStream != null) {
            // Need to flush buffer to zstd if encoded buffer data is full
            zstdDirectBufferCompressingStream.compress(encodedDataBuffer);
        }
        encodedDataBuffer.putChar(val);
    }

    @Override
    public void putInt(int val) throws IOException {
        if (encodedDataBuffer.remaining() < Integer.SIZE & zstdDirectBufferCompressingStream != null) {
            // Need to flush buffer to zstd if encoded buffer data is full
            zstdDirectBufferCompressingStream.compress(encodedDataBuffer);
        }
        encodedDataBuffer.putInt(val);
    }

    @Override
    public void putLong(long val) throws IOException {
        if (encodedDataBuffer.remaining() < Long.SIZE & zstdDirectBufferCompressingStream != null) {
            // Need to flush buffer to zstd if encoded buffer data is full
            zstdDirectBufferCompressingStream.compress(encodedDataBuffer);
        }
        encodedDataBuffer.putLong(val);
    }
}
