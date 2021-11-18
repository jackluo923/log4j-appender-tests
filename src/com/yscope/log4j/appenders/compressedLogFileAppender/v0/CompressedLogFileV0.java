package com.yscope.log4j.appenders.compressedLogFileAppender.v0;

import com.github.luben.zstd.ZstdDirectBufferCompressingStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Warning: Does not support manager reconfiguration
 */
@Plugin(name = "CompressedLogFileV0", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class CompressedLogFileV0 extends AbstractAppender {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();

    // Log type and Variable Dictionaries
    // We use Short type for logtypeId and Integer type for varId to save memory
    // When flushing entries to memory-mapped region, we'll flush logtypeIds as short and varIds as long
    private final HashMap<ByteBuffer, Short> logtypeDict = new HashMap<>();
    private Short nextLogtypeId = 0;
    private final HashMap<ByteBuffer, Integer> varDict = new HashMap<>();
    private Integer nextVarId = 0;

    // Log type and variable type dictionary and the three data columns mmapped buffer
    FileChannel logtypeDictFC;
    FileChannel varDictFC;
    FileChannel tsFC;
    FileChannel logtypeFC;
    FileChannel varFC;
    MappedByteBuffer logtypeDictBuf;
    MappedByteBuffer varDictBuf;
    MappedByteBuffer tsBuf;
    MappedByteBuffer logtypeBuf;
    MappedByteBuffer varBuf;

    private Path compressedLogFile;
    private Path compressedLogDir;
    private Integer bufferSize;
    private Integer compressionLevel;

    private final boolean enableDebugOutput;
    private final boolean enableCompression;

    private CompressedLogFileV0(String name, Filter filter,
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

            // Initialize memory mapped files
            logtypeDictFC = FileChannel.open(compressedLogDir.resolve("logtype.dict"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            logtypeDictBuf = logtypeDictFC.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
            varDictFC = FileChannel.open(compressedLogDir.resolve("var.dict"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            varDictBuf = varDictFC.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
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
    public static CompressedLogFileV0 createAppender(
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

        return new CompressedLogFileV0(name, filter, compressedLogPatternLayoutContainer,
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
                    compressedLogFC.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
            final ZstdDirectBufferCompressingStream zstdDirectBufferCompressingStream
                    = new ZstdDirectBufferCompressingStream(compressedLogBuf, compressionLevel);
            zstdDirectBufferCompressingStream.compress(logtypeDictBuf.flip());
            zstdDirectBufferCompressingStream.compress(varDictBuf.flip());
            zstdDirectBufferCompressingStream.compress(tsBuf.flip());
            zstdDirectBufferCompressingStream.compress(logtypeBuf.flip());
            zstdDirectBufferCompressingStream.compress(varBuf.flip());
            zstdDirectBufferCompressingStream.flush();
            truncateMemoryMappedFile(compressedLogBuf, compressedLogFC);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void truncateAllMemoryMappedFiles() {
        try {
            truncateMemoryMappedFile(logtypeDictBuf, logtypeDictFC);
            truncateMemoryMappedFile(varDictBuf, varDictFC);
            truncateMemoryMappedFile(tsBuf, tsFC);
            truncateMemoryMappedFile(logtypeBuf, logtypeFC);
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

    private Short getLogTypeId(ByteBuffer logtype) {
        // Upsert logtype into logtype dictionary
        Short logtypeId = logtypeDict.putIfAbsent(logtype, nextLogtypeId);
        if (null == logtypeId) {
            logtypeId = nextLogtypeId;
            nextLogtypeId++;

            // Serialize and persist log type dictionary with new logtype to memory mapped file
            // Format: <unsigned short length><string><unsigned short length><string>...
            // Java doesn't have unsigned short types except for char (16bit), therefore we'll cast it to char
            logtypeDictBuf.putChar((char) logtype.capacity());
            logtypeDictBuf.put(logtype.array());
        }

        return logtypeId;
    }

    private Integer getVarId(ByteBuffer var) {
        // Upsert var into var dictionary
        Integer varId = varDict.putIfAbsent(var, nextVarId);
        if (null == varId) {
            varId = nextVarId;
            nextVarId++;

            // Serialize and persist var dictionary with new var to memory mapped file
            // Format: <short length><string><short length><string>...
            // Java doesn't have unsigned short types except for char (16bit), therefore we'll cast it to char
            varDictBuf.putChar((char) var.capacity());
            varDictBuf.put(var.array());
        }

        return varId;
    }

    private static class TokenBounds {
        public int beginPos = 0;
        public int endPos = 0;
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

    private static boolean getBoundsOfNextVar(byte[] msg, TokenBounds tokenBounds) {
        if (tokenBounds.endPos >= msg.length) {
            return false;
        }

        boolean isVar = false;
        while (!isVar) {
            tokenBounds.beginPos = tokenBounds.endPos;
            // Find next non-delimiter
            for (; tokenBounds.beginPos < msg.length; ++tokenBounds.beginPos) {
                byte c = msg[tokenBounds.beginPos];

                if (!isDelimiter(c)) {
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
                isVar = true;
            }
        }

        return true;
    }

    private static boolean parseNextVar(byte[] msg, TokenBounds tokenBounds,
                                        ByteArrayOutputStream logtypeStream, ByteArrayOutputStream varStream) {
        int lastVarEndPos = tokenBounds.endPos;
        if (getBoundsOfNextVar(msg, tokenBounds)) {
            logtypeStream.write(msg, lastVarEndPos, tokenBounds.beginPos - lastVarEndPos);

            varStream.reset();
            varStream.write(msg, tokenBounds.beginPos, tokenBounds.endPos - tokenBounds.beginPos);
            return true;
        }
        if (lastVarEndPos < msg.length) {
            logtypeStream.write(msg, lastVarEndPos, tokenBounds.beginPos - lastVarEndPos);
        }

        return false;
    }

    private Long convertByteArrayToRepresentableIntegerVar(ByteBuffer var) {
        final byte[] bytes = var.array();
        if (bytes.length == 0) {
            // Empty string cannot be converted
            return null;
        }

        // Ensure start of value is an integer with no zero-padding or positive sign
        if ('-' == bytes[0]) {
            // Ensure first character after sign is a non-zero integer
            if (bytes.length < 2 || bytes[1] < '1' || '9' < bytes[1]) {
                return null;
            }
        } else {
            // Ensure first character is a digit
            if (bytes[0] < '0' || '9' < bytes[0]) {
                return null;
            }

            // Ensure value is not zero-padded
            if (bytes.length > 1 && '0' == bytes[0]) {
                return null;
            }
        }

        return convertByteArrayToLong(bytes);
    }

    private static Long convertByteArrayToLong(byte[] bytes) {
        try {
            return Long.parseLong(new String(bytes));
        } catch (NumberFormatException e) {
            return null;
        }
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

            TokenBounds tokenBounds = new TokenBounds();
            ByteArrayOutputStream logTypeStream = new ByteArrayOutputStream();
            ByteArrayOutputStream varStream = new ByteArrayOutputStream();
            while (parseNextVar(logMessage, tokenBounds, logTypeStream, varStream)) {
                long encodedVar;
                ByteBuffer var = ByteBuffer.wrap(varStream.toByteArray());
                Long varAsLong = convertByteArrayToRepresentableIntegerVar(var);
                if (null != varAsLong) {
                    // Encode as long
                    logTypeStream.write((char) 17);
                    encodedVar = varAsLong;
                } else {
                    logTypeStream.write((char) 18);
                    encodedVar = getVarId(var);
                }
                varBuf.putLong(encodedVar);
            }
            ByteBuffer logType = ByteBuffer.wrap(logTypeStream.toByteArray());
            logtypeBuf.putShort(getLogTypeId(logType));

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
