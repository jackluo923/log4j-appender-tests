package com.yscope.log4j.appenders.compressedLogFileAppender.v1;

import com.github.luben.zstd.ZstdDirectBufferCompressingStream;
import com.yscope.log4j.appenders.compressedLogFileAppender.v1.binaryDataWrappers.LogtypeByteArrayView;
import com.yscope.log4j.appenders.compressedLogFileAppender.v1.binaryDataWrappers.VariableByteArrayView;
import com.yscope.log4j.appenders.compressedLogFileAppender.v1.compressionDictionary.LogtypeCompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v1.compressionDictionary.VariableCompressionDictionary;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * v1 - improved memory utilization + garbage generation
 * - Perform "zero-copy" parsing and dictionary lookup
 * - Switch to fastutil Object2IntegerOpenHashMap for lower memory overhead
 * Warning: Does not support manager reconfiguration
 */
@Plugin(name = "CompressedLogFileV1", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class CompressedLogFileV1 extends AbstractAppender {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();

    // Log type and Variable Dictionaries
    private LogtypeCompressionDictionary logtypeDict;
    private VariableCompressionDictionary varDict;

    // Log type and variable type dictionary and the three data columns mmapped buffer
    private FileChannel logtypeDictFC;
    private FileChannel varDictFC;
    private FileChannel tsFC;
    private FileChannel logtypeFC;
    private FileChannel varFC;
    private MappedByteBuffer logtypeDictBuf;
    private MappedByteBuffer varDictBuf;
    private MappedByteBuffer tsBuf;
    private MappedByteBuffer logtypeBuf;
    private MappedByteBuffer varBuf;

    private Path compressedLogFile;
    private Path compressedLogDir;
    private Integer bufferSize;
    private Integer compressionLevel;

    private final boolean enableDebugOutput;
    private final boolean enableCompression;

    private ByteBuffer logtype = ByteBuffer.allocate(256);
    private final TokenBounds tokenBounds = new TokenBounds();

    private CompressedLogFileV1(String name, Filter filter,
                                CompressedLogFilePatternLayoutContainer compressedLogPatternLayoutContainer,
                                String fileName, Integer bufferSize,
                                Boolean enableCompression, Integer compressionLevel,
                                Boolean enableDebugOutput, final boolean ignoreExceptions) {
        super(name, filter, compressedLogPatternLayoutContainer.getCompressedLogPatternLayout(), ignoreExceptions);
        this.enableDebugOutput = enableDebugOutput;
        this.enableCompression = enableCompression;
        this.bufferSize = bufferSize;
        this.compressionLevel = compressionLevel;


        try {
            // Create directory for log file if not exist
            compressedLogFile = Path.of(fileName);
            compressedLogDir = compressedLogFile.getParent();
            Files.createDirectories(compressedLogDir);

            // Initialize compression dictionaries as well as all required memory mapped backing files
            logtypeDictFC = FileChannel.open(compressedLogDir.resolve("logtype.dict"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            logtypeDictBuf = logtypeDictFC.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
            logtypeDict = new LogtypeCompressionDictionary(logtypeDictBuf);

            varDictFC = FileChannel.open(compressedLogDir.resolve("var.dict"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            varDictBuf = varDictFC.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
            varDict = new VariableCompressionDictionary(varDictBuf);

            tsFC = FileChannel.open(compressedLogDir.resolve("ts.bin"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            tsBuf = tsFC.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
            logtypeFC = FileChannel.open(compressedLogDir.resolve("logtype.bin"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            logtypeBuf = logtypeFC.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
            varFC = FileChannel.open(compressedLogDir.resolve("var.bin"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            varBuf = varFC.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PluginFactory
    public static CompressedLogFileV1 createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter,
            @PluginAttribute("fileName") final String fileName,
            @PluginAttribute("bufferSize") Integer bufferSize,
            @PluginAttribute("enableCompression") Boolean enableCompression,
            @PluginAttribute("compressionLevel") Integer compressionLevel,
            @PluginAttribute("enableDebugOutput") Boolean enableDebugOutput,
            @PluginAttribute("ignoreExceptions") Boolean ignoreExceptions
    ) {
        if (name == null) {
            LOGGER.error("No name provided for CLPAppenderImpl");
            return null;
        }
        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }

        CompressedLogFilePatternLayoutContainer compressedLogPatternLayoutContainer =
                new CompressedLogFilePatternLayoutContainer((PatternLayout) layout);

        if (fileName == null) {
            LOGGER.error("No file name provided for CompressedLogAppenderV0");
            return null;
        }
        if (bufferSize == null) {
            bufferSize = 64 * 1024 * 1024;   // use 64MB by default
        }
        if (enableCompression == null) {
            enableCompression = false;
        }
        if (compressionLevel == null) {
            compressionLevel = 3;
        }
        if (enableDebugOutput == null) {
            enableDebugOutput = false;
        }
        if (ignoreExceptions == null) {
            ignoreExceptions = true;
        }

        return new CompressedLogFileV1(name, filter, compressedLogPatternLayoutContainer,
                fileName, bufferSize, enableCompression, compressionLevel, enableDebugOutput, ignoreExceptions);
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        if (enableCompression) {
            compressIntermediateRepresentation();
        }
        truncateAllMemoryMappedFiles();
        System.out.println("Gracefully Stopped " +  this.getName() + " appender");
        return true;
    }

    private void compressIntermediateRepresentation() {
        try {
            FileChannel compressedLogFC = FileChannel.open(compressedLogFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            MappedByteBuffer compressedLogBuf =
                    compressedLogFC.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize * 2);
            final ZstdDirectBufferCompressingStream zstdDirectBufferCompressingStream
                    = new ZstdDirectBufferCompressingStream(compressedLogBuf, compressionLevel);
            zstdDirectBufferCompressingStream.compress(logtypeDictBuf.flip());
            zstdDirectBufferCompressingStream.compress(varDictBuf.flip());
            zstdDirectBufferCompressingStream.compress(tsBuf.flip());
            zstdDirectBufferCompressingStream.compress(logtypeBuf.flip());
            zstdDirectBufferCompressingStream.compress(varBuf.flip());
            zstdDirectBufferCompressingStream.flush();
            zstdDirectBufferCompressingStream.close();
            truncateMemoryMappedFile(compressedLogBuf, compressedLogFC);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void truncateAllMemoryMappedFiles() {
        try {
            System.out.println("Flushing and truncating logtypeDictBuf");
            truncateMemoryMappedFile(logtypeDictBuf, logtypeDictFC);
            System.out.println("Flushing and truncating varDictBuf");
            truncateMemoryMappedFile(varDictBuf, varDictFC);
            System.out.println("Flushing and truncating tsBuf");
            truncateMemoryMappedFile(tsBuf, tsFC);
            System.out.println("Flushing and truncating logtypeBuf");
            truncateMemoryMappedFile(logtypeBuf, logtypeFC);
            System.out.println("Flushing and truncating varBuf");
            truncateMemoryMappedFile(varBuf, varFC);
            System.out.println("Successfully truncated memory mapped files");
        } catch (IOException e) {
            System.out.println("Failed to truncate memory mapped files");
            e.printStackTrace();
        }
    }

    private int truncateMemoryMappedFile(ByteBuffer buf, FileChannel fc) throws IOException {
        final int position = buf.position();
        // Suggest JVM to perform system garbage collection as a precaution
        // Warning: this may not always be reliable, therefore we must implement it via JNI
        System.gc();
        fc.truncate(position);
        fc.close();
        return position;
    }

    private static class TokenBounds {
        public int beginPos = 0;
        public int endPos = 0;

        public void reset() {
            beginPos = 0;
            endPos = 0;
        }
    }

    private static boolean isDelimiter(byte c) {
        return !('+' == c || ('-' <= c && c <= '9') || ('A' <= c && c <= 'Z') || '\\' == c || '_' == c || ('a' <= c && c <= 'z'));
    }

    private static boolean couldBeMultiDigitHexValue(byte[] str, int beginPos, int endPos) {
        if (endPos - beginPos < 2) {
            return false;
        }

        for (int i = beginPos; i < endPos; ++i) {
            byte c = str[i];
            if (!(('a' <= c && c <= 'f') || ('A' <= c && c <= 'F') || ('0' <= c && c <= '9'))) {
                return false;
            }
        }

        return true;
    }

    private boolean getBoundsOfNextVar(byte[] msg) {
        if (tokenBounds.endPos >= msg.length) {
            return false;
        }

        while (true) {
            tokenBounds.beginPos = tokenBounds.endPos;
            // Find next non-delimiter
            for (; tokenBounds.beginPos < msg.length; ++tokenBounds.beginPos) {
                if (!isDelimiter(msg[tokenBounds.beginPos])) {
                    break;
                }
            }
            if (msg.length == tokenBounds.beginPos) {
                // Early exit for performance
                return false;
            }

            boolean containsDecimalDigit = false;
            boolean containsAlphabet = false;

            // Find next delimiter
            tokenBounds.endPos = tokenBounds.beginPos;
            for (; tokenBounds.endPos < msg.length; ++tokenBounds.endPos) {
                byte c = msg[tokenBounds.endPos];
                if ('0' <= c && c <= '9') {
                    // Contains number, so treat token as a variable
                    containsDecimalDigit = true;
                } else if (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')) {
                    containsAlphabet = true;
                } else if (isDelimiter(c)) {
                    break;
                }
            }

            if (containsDecimalDigit ||
                    (tokenBounds.beginPos > 0 && '=' == msg[tokenBounds.beginPos - 1] && containsAlphabet) ||
                    couldBeMultiDigitHexValue(msg, tokenBounds.beginPos, tokenBounds.endPos)) {
                break;
            }
        }

        return true;
    }

    private boolean parseNextVar(byte[] msg) {
        int lastVarEndPos = tokenBounds.endPos;
        if (getBoundsOfNextVar(msg)) {
            logtype.put(msg, lastVarEndPos, tokenBounds.beginPos - lastVarEndPos);
            return true;
        }
        if (lastVarEndPos < msg.length) {
            logtype.put(msg, lastVarEndPos, tokenBounds.beginPos - lastVarEndPos);
        }
        return false;
    }

    @Override
    public void append(LogEvent event) {
        // Using a global lock for now. Locks are to handle when multiple threads print log messages
        // Lock is only really needed in 2 conditions:
        // 1) when we insert into hash table and writing out values (via mmap)
        // 2) when we insert multiple variables into variable column
        readLock.lock();
        try {
            // Insert into timestamp column right away, no parsing required
            // Will encode the format into metadata in future implementation
            tsBuf.putLong(event.getTimeMillis());

            // Parsing log message. Note log message here does not contain timestamp
            byte[] logMessage = getLayout().toByteArray(event);

            // Reset (and potentially extend) buffers and token bounds
            if (logtype.capacity() < logMessage.length) {
                logtype = ByteBuffer.allocate(logMessage.length + 128);   // Extend a bit more
            }
            logtype.rewind();
            tokenBounds.reset();

             // Logtype must be equal or shorter than msg
            while (parseNextVar(logMessage)) {
                VariableByteArrayView var =
                        new VariableByteArrayView(logMessage, tokenBounds.beginPos, tokenBounds.endPos);
                if (var.parseLong()) {
                    // Variable can be represented by a primitive long type
                    logtype.put((byte) 17);   // Delimiter for native numerical type
                    varBuf.putLong(var.longRepresentation);
                } else {
                    // Variable must be encoded
                    logtype.put((byte) 18);   // Delimiter for id type
                    varBuf.putLong(varDict.getVariableId(var));
                }
            }
            final LogtypeByteArrayView logtypeByteBufferWrapper = new LogtypeByteArrayView(logtype);
            logtypeBuf.putChar(logtypeDict.getLogtypeIdCompact(logtypeByteBufferWrapper));

            if (enableDebugOutput) {
                System.out.write(logMessage);
            }
        } catch (Exception ex) {
            if (!ignoreExceptions()) {
                throw new AppenderLoggingException(ex);
            }
        } finally {
            readLock.unlock();
        }
    }
}
