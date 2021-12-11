package com.yscope.log4j.appenders.compressedLogFileAppender.v5.compressionDictionary;

import com.github.luben.zstd.ZstdDirectBufferCompressingStream;
import com.yscope.log4j.appenders.compressedLogFileAppender.v5.compressionDictionary.key.ByteArrayViewDictionaryKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MemoryMappedCompressionDictionary extends CompressionDictionary {
    private final FileChannel fc;
    private final ByteBuffer encodedDataBuffer;
    private final MappedByteBuffer memoryMappedBuffer;
    private ZstdDirectBufferCompressingStream zstdDirectBufferCompressingStream = null;

    public MemoryMappedCompressionDictionary(final Path path, int maxBufSize, int compressionLevel)
            throws IOException {
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

    @Override
    public void close() throws IOException {
        if (zstdDirectBufferCompressingStream != null) {
            // Flush encoded data cache to zstd, compress and close
            zstdDirectBufferCompressingStream.compress(encodedDataBuffer);
            zstdDirectBufferCompressingStream.close();
        }
        // Close memory mapped file buffer
        final int position = memoryMappedBuffer.position();
        // TODO: It's possible this call may fail because the buffer is not unmapped. In that case,
        // we may need to call System.gc() or use a more advanced method of forcing it to be
        // unmapped.
        fc.truncate(position);
        fc.close();
    }

    @Override
    protected void persistDictionaryEntry(ByteArrayViewDictionaryKey byteArrayViewDictionaryKey) throws IOException {
        // Serialize and persist var dictionary with new id to memory mapped file
        // Format: <unsigned short length><utf8 string><unsigned short length><utf8 string>...
        // Java doesn't have unsigned short types except for char (16bit), therefore we cast it to char
        int dictionaryKeyLength = byteArrayViewDictionaryKey.getViewSize();
        if (dictionaryKeyLength > Character.MAX_VALUE) {
            throw new StringIndexOutOfBoundsException("Dictionary key's length exceeds encoding capability");
        }
        encodedDataBuffer.putChar((char) dictionaryKeyLength);
        encodedDataBuffer.put(byteArrayViewDictionaryKey.bytes);
        zstdDirectBufferCompressingStream.compress(encodedDataBuffer);
    }
}
