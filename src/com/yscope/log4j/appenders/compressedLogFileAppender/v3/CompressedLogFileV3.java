package com.yscope.log4j.appenders.compressedLogFileAppender.v3;

import com.github.luben.zstd.ZstdDirectBufferCompressingStream;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.CompactVariableByteArrayView;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.VariableByteArrayView;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers.LogtypeByteArrayView;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.compressionDictionary.BufferedLogtypeCompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.compressionDictionary.BufferedVariableCompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.compressionDictionary.LogtypeCompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.compressionDictionary.VariableCompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v3.utilityClasses.PatternLayoutBufferDestination;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.*;
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
 * v3 - Adding floating point variable encoding support on top of V1 implementation.
 */
@Plugin(name = "CompressedLogFileV3", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class CompressedLogFileV3 extends AbstractAppender {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();

    private Integer bufferSize;


    private final boolean useCompactVariableEncoding;

    private final boolean enableCompression;
    private Integer compressionLevel;

    private final boolean useMemoryMappedIO;

    // Buffers for memory mapped file IO (default)
    private LogtypeCompressionDictionary logtypeDict;
    private VariableCompressionDictionary varDict;
    private Path compressedLogFile;
    private Path compressedLogDir;
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

    // BufferedOutputStreams for buffered file IO
    private BufferedLogtypeCompressionDictionary bufferedLogtypeDict;
    private BufferedVariableCompressionDictionary bufferedVarDict;
    private DataOutputStream logtypeDictBDOS;
    private DataOutputStream varDictBDOS;
    private DataOutputStream tsBDOS;
    private DataOutputStream logtypeBDOS;
    private DataOutputStream varBDOS;

    private final boolean enableDebugOutput;

    private PatternLayoutBufferDestination patternLayoutBufferDestination;

    private ByteBuffer logtype;
    final LogtypeByteArrayView logtypeByteArrayView;
    private VariableByteArrayView variableByteArrayView;
    private CompactVariableByteArrayView compactVariableByteArrayView;

    private final TokenBounds tokenBounds;

    private CompressedLogFileV3(String name, Filter filter,
                                CompressedLogFilePatternLayoutContainer compressedLogPatternLayoutContainer,
                                String fileName, Integer bufferSize,
                                Boolean enableCompression, Integer compressionLevel, Boolean useCompactVariableEncoding,
                                Boolean useMemoryMappedIO, Boolean enableDebugOutput, final boolean ignoreExceptions) {
        super(name, filter, compressedLogPatternLayoutContainer.getCompressedLogPatternLayout(), ignoreExceptions);
        this.enableCompression = enableCompression;
        this.compressionLevel = compressionLevel;
        this.useCompactVariableEncoding = useCompactVariableEncoding;
        this.useMemoryMappedIO = useMemoryMappedIO;
        this.enableDebugOutput = enableDebugOutput;
        this.bufferSize = bufferSize;
        this.patternLayoutBufferDestination = new PatternLayoutBufferDestination(8192);

        logtype = ByteBuffer.allocate(8192);
        logtypeByteArrayView = new LogtypeByteArrayView(logtype);
        tokenBounds = new TokenBounds();
        if (useCompactVariableEncoding) {
            compactVariableByteArrayView = new CompactVariableByteArrayView(
                    this.patternLayoutBufferDestination.getByteBuffer(), 0,
                    this.patternLayoutBufferDestination.getByteBuffer().position());
        } else {
            variableByteArrayView = new VariableByteArrayView(
                    this.patternLayoutBufferDestination.getByteBuffer(), 0,
                    this.patternLayoutBufferDestination.getByteBuffer().position());
        }

        try {
            // Create directory for log file if not exist
            compressedLogFile = Path.of(fileName);
            compressedLogDir = compressedLogFile.getParent();
            Files.createDirectories(compressedLogDir);

            if (useMemoryMappedIO) {
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
            } else {
                logtypeDictBDOS = new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(String.valueOf(compressedLogDir.resolve("logtype.dict")))));
                bufferedLogtypeDict = new BufferedLogtypeCompressionDictionary(logtypeDictBDOS);
                varDictBDOS = new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(String.valueOf(compressedLogDir.resolve("var.dict")))));
                bufferedVarDict = new BufferedVariableCompressionDictionary(varDictBDOS);
                tsBDOS = new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(String.valueOf(compressedLogDir.resolve("ts.bin")))));
                logtypeBDOS = new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(String.valueOf(compressedLogDir.resolve("logtype.bin")))));
                varBDOS = new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(String.valueOf(compressedLogDir.resolve("var.bin")))));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PluginFactory
    public static CompressedLogFileV3 createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter,
            @PluginAttribute("fileName") final String fileName,
            @PluginAttribute("bufferSize") Integer bufferSize,
            @PluginAttribute("enableCompression") Boolean enableCompression,
            @PluginAttribute("compressionLevel") Integer compressionLevel,
            @PluginAttribute("useCompactVariableEncoding") Boolean useCompactVariableEncoding,
            @PluginAttribute("useMemoryMappedIO") Boolean useMemoryMappedIO,
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
        if (useCompactVariableEncoding == null) {
            useCompactVariableEncoding = false;
        }
        if (useMemoryMappedIO == null) {
            useMemoryMappedIO = true;
        }
        if (enableDebugOutput == null) {
            enableDebugOutput = false;
        }
        if (ignoreExceptions == null) {
            ignoreExceptions = true;
        }

        return new CompressedLogFileV3(name, filter, compressedLogPatternLayoutContainer,
                fileName, bufferSize, enableCompression, compressionLevel, useCompactVariableEncoding,
                useMemoryMappedIO, enableDebugOutput, ignoreExceptions);
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        if (useMemoryMappedIO) {
            if (enableCompression) {
                compressIntermediateRepresentation();
            }
            truncateAllMemoryMappedFiles();
        } else {
            try {
                logtypeDictBDOS.close();
                varDictBDOS.close();
                tsBDOS.close();
                logtypeBDOS.close();
                varBDOS.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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

    private boolean getBoundsOfNextVar(ByteBuffer logMessage) {
        if (tokenBounds.endPos >= logMessage.position()) {
            return false;
        }

        while (true) {
            tokenBounds.beginPos = tokenBounds.endPos;
            // Find next non-delimiter
            for (; tokenBounds.beginPos < logMessage.position(); ++tokenBounds.beginPos) {
                if (!isDelimiter(logMessage.get(tokenBounds.beginPos))) {
                    break;
                }
            }
            if (logMessage.position() == tokenBounds.beginPos) {
                // Early exit for performance
                return false;
            }

            boolean containsDecimalDigit = false;
            boolean containsAlphabet = false;

            // Find next delimiter
            tokenBounds.endPos = tokenBounds.beginPos;
            for (; tokenBounds.endPos < logMessage.position(); ++tokenBounds.endPos) {
                byte c = logMessage.get(tokenBounds.endPos);
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
                    (tokenBounds.beginPos > 0 && '=' == logMessage.get(tokenBounds.beginPos - 1) && containsAlphabet) ||
                    couldBeMultiDigitHexValue(logMessage.array(), tokenBounds.beginPos, tokenBounds.endPos)) {
                break;
            }
        }

        return true;
    }

    private boolean parseNextVar(ByteBuffer logMessage) {
        int lastVarEndPos = tokenBounds.endPos;
        if (getBoundsOfNextVar(logMessage)) {
            logtype.put(logMessage.array(), lastVarEndPos, tokenBounds.beginPos - lastVarEndPos);
            return true;
        } else {
            logtype.put(logMessage.array(), lastVarEndPos, tokenBounds.beginPos - lastVarEndPos);
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
            logtype.rewind();
            tokenBounds.reset();
            patternLayoutBufferDestination.getByteBuffer().clear();

            // Parsing log message. Note log message here does not contain timestamp
            // Parse log message into destination buffer
            getLayout().encode(event, patternLayoutBufferDestination);
            ByteBuffer logMessage = patternLayoutBufferDestination.getByteBuffer();

            // Reset (and potentially extend) buffers and token bounds
            if (logtype.capacity() < logMessage.capacity()) {
                logtype = ByteBuffer.allocate(logMessage.capacity());
                if (useCompactVariableEncoding) {
                    compactVariableByteArrayView.bytes = logMessage.array();
                } else {
                    variableByteArrayView.bytes = logMessage.array();
                }
            }
            logtypeByteArrayView.setBounds(0, logMessage.position());

            if (enableDebugOutput) {
                System.out.write(logMessage.array(), 0, logMessage.position());
            }

            if (useMemoryMappedIO) {
                // Insert into timestamp column right away, no parsing required
                // Will encode the format into metadata in future implementation
                tsBuf.putLong(event.getTimeMillis());

                // Logtype must be equal or shorter than msg
                if (useCompactVariableEncoding) {
                    while (parseNextVar(logMessage)) {
                        compactVariableByteArrayView.setBounds(tokenBounds.beginPos, tokenBounds.endPos);
                        if (compactVariableByteArrayView.encodeAs32bitIntegerType()) {
                            logtype.put((byte) 17);   // Delimiter for unsigned long numerical type
                            varBuf.putInt(compactVariableByteArrayView.signedIntBackedEncoding);
                        } else if (compactVariableByteArrayView.encodeAs32BitDecimalType()) {
                            logtype.put((byte) 19);   // Delimiter for custom decimal encoding type
                            varBuf.putInt(compactVariableByteArrayView.signedIntBackedEncoding);
                        } else {
                            // Variable must be encoded
                            logtype.put((byte) 18);   // Delimiter for id type
                            varBuf.putInt(varDict.getVariableId(compactVariableByteArrayView));
                        }
                    }
                } else {
                    while (parseNextVar(logMessage)) {
                        variableByteArrayView.setBounds(tokenBounds.beginPos, tokenBounds.endPos);
                        if (variableByteArrayView.encodeAs64bitIntegerType()) {
                            logtype.put((byte) 17);   // Delimiter for unsigned long numerical type
                            varBuf.putLong(variableByteArrayView.signedLongBackedEncoding);
                        } else if (variableByteArrayView.encodeAs64bitDecimalType()) {
                            logtype.put((byte) 19);   // Delimiter for custom decimal encoding type
                            varBuf.putLong(variableByteArrayView.signedLongBackedEncoding);
                        } else {
                            // Variable must be encoded
                            logtype.put((byte) 18);   // Delimiter for id type
                            varBuf.putLong(varDict.getVariableId(variableByteArrayView));
                        }
                    }
                }
                logtypeBuf.putChar(logtypeDict.getLogtypeIdCompact(logtypeByteArrayView));
            } else {
                // Insert into timestamp column right away, no parsing required
                // Will encode the format into metadata in future implementation
                tsBDOS.writeLong(event.getTimeMillis());

                // Logtype must be equal or shorter than msg
                if (useCompactVariableEncoding) {
                    while (parseNextVar(logMessage)) {
                        compactVariableByteArrayView.setBounds(tokenBounds.beginPos, tokenBounds.endPos);
                        if (compactVariableByteArrayView.encodeAs32bitIntegerType()) {
                            logtype.put((byte) 17);   // Delimiter for unsigned long numerical type
                            varBDOS.writeInt(compactVariableByteArrayView.signedIntBackedEncoding);
                        } else if (compactVariableByteArrayView.encodeAs32BitDecimalType()) {
                            logtype.put((byte) 19);   // Delimiter for custom decimal encoding type
                            varBDOS.writeInt(compactVariableByteArrayView.signedIntBackedEncoding);
                        } else {
                            // Variable must be encoded
                            logtype.put((byte) 18);   // Delimiter for id type
                            varBDOS.writeInt(bufferedVarDict.getVariableId(compactVariableByteArrayView));
                        }
                    }
                } else {
                    while (parseNextVar(logMessage)) {
                        variableByteArrayView.setBounds(tokenBounds.beginPos, tokenBounds.endPos);
                        if (variableByteArrayView.encodeAs64bitIntegerType()) {
                            logtype.put((byte) 17);   // Delimiter for unsigned long numerical type
                            varBDOS.writeLong(variableByteArrayView.signedLongBackedEncoding);
                        } else if (variableByteArrayView.encodeAs64bitDecimalType()) {
                            logtype.put((byte) 19);   // Delimiter for custom decimal encoding type
                            varBDOS.writeLong(variableByteArrayView.signedLongBackedEncoding);
                        } else {
                            // Variable must be encoded
                            logtype.put((byte) 18);   // Delimiter for id type
                            varBDOS.writeLong(bufferedVarDict.getVariableId(variableByteArrayView));
                        }
                    }
                }
                logtypeBDOS.writeChar(bufferedLogtypeDict.getLogtypeIdCompact(logtypeByteArrayView));
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
