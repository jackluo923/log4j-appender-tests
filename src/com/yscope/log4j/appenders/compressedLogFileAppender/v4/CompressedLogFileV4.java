package com.yscope.log4j.appenders.compressedLogFileAppender.v4;

import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.BufferedDataOutputStreamCompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.CompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.MemoryMappedCompressionDictionary;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key.CompactVariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key.LogtypeByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key.StandardVariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key.VariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.logtype.LogtypeBufferedFileOutputStreamIR;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.logtype.LogtypeIR;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.logtype.LogtypeMemoryMappedIR;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.timestamp.TimestampBufferedFileOutputStreamIR;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.timestamp.TimestampIR;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.timestamp.TimestampMemoryMappedIR;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.intermediateRepresentation.variable.*;
import com.yscope.log4j.appenders.compressedLogFileAppender.v4.utilityClasses.PatternLayoutBufferDestination;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

/**
 * v4 - Adding sha256 based dictionary and prepare for socket appender implementation
 */
@Plugin(name = "CompressedLogFileV4", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class CompressedLogFileV4 extends AbstractAppender {
    private Integer bufferSize;
    private final boolean useCompactVariableEncoding;
    private Integer compressionLevel;
    private final boolean useMemoryMappedIO;
    private CompressionDictionary logtypeDict;
    private CompressionDictionary variableDict;
    private TimestampIR timestampIR;
    private LogtypeIR logtypeIR;
    private VariableIR variableIR;
    private Path compressedLogFile;
    private Path compressedLogDir;

    private boolean enableDebugOutput;

    private PatternLayoutBufferDestination patternLayoutBufferDestination;

    VariableByteArrayViewDictionaryKey variableDictionaryKey;
    LogtypeByteArrayViewDictionaryKey logtypeDictionaryKey;

    private ByteBuffer logtype;
    private TokenBounds tokenBounds;

    private String logtypeDictFilename = "logtype.dict";
    private String variableDictFilename = "variable.dict";
    private String timestampIRFilename = "ts.bin";
    private String logtypeIRFilename = "logtype.bin";
    private String variableIRFilename = "var.bin";

    private CompressedLogFileV4(String name, Filter filter,
                                CompressedLogFilePatternLayoutContainer compressedLogPatternLayoutContainer,
                                String fileName, int bufferSize, int compressionLevel,
                                boolean useCompactVariableEncoding, boolean useMemoryMappedIO,
                                boolean enableDebugOutput, final boolean ignoreExceptions) {
        super(name, filter, compressedLogPatternLayoutContainer.getCompressedLogPatternLayout(), ignoreExceptions, Property.EMPTY_ARRAY);
        this.compressionLevel = compressionLevel;
        this.useCompactVariableEncoding = useCompactVariableEncoding;
        this.useMemoryMappedIO = useMemoryMappedIO;
        this.enableDebugOutput = enableDebugOutput;
        this.bufferSize = bufferSize;
        this.patternLayoutBufferDestination = new PatternLayoutBufferDestination(8192);

        logtype = ByteBuffer.allocate(8192);
        tokenBounds = new TokenBounds();

        try {
            // Create directory for log file if it doesn't exist
            compressedLogFile = Path.of(fileName);
            compressedLogDir = compressedLogFile.getParent();
            Files.createDirectories(compressedLogDir);

            if (compressionLevel != 0) {
                logtypeDictFilename += ".zst";
                variableDictFilename += ".zst";
                timestampIRFilename += ".zst";
                logtypeIRFilename += ".zst";
                variableIRFilename += ".zst";
            }

            if (useMemoryMappedIO) {
                logtypeDict = new MemoryMappedCompressionDictionary(
                        compressedLogDir.resolve(logtypeDictFilename), bufferSize, compressionLevel);
                variableDict = new MemoryMappedCompressionDictionary(
                        compressedLogDir.resolve(variableDictFilename), bufferSize, compressionLevel);
                timestampIR = new TimestampMemoryMappedIR(
                        compressedLogDir.resolve(timestampIRFilename), bufferSize, compressionLevel);
                logtypeIR = new LogtypeMemoryMappedIR(
                        compressedLogDir.resolve(logtypeIRFilename), bufferSize, compressionLevel);
                if (useCompactVariableEncoding) {
                    variableIR = new CompactVariableMemoryMappedIR(
                            compressedLogDir.resolve(variableIRFilename), bufferSize, compressionLevel);
                } else {
                    variableIR = new StandardVariableMemoryMappedIR(
                            compressedLogDir.resolve(variableIRFilename), bufferSize, compressionLevel);
                }
            } else {
                logtypeDict = new BufferedDataOutputStreamCompressionDictionary(
                        compressedLogDir.resolve(logtypeDictFilename), compressionLevel);
                variableDict = new BufferedDataOutputStreamCompressionDictionary(
                        compressedLogDir.resolve(variableDictFilename), compressionLevel);
                timestampIR = new TimestampBufferedFileOutputStreamIR(
                        compressedLogDir.resolve(timestampIRFilename), compressionLevel);
                logtypeIR = new LogtypeBufferedFileOutputStreamIR(
                        compressedLogDir.resolve(logtypeIRFilename), compressionLevel);
                if (useCompactVariableEncoding) {
                    variableIR = new CompactVariableBufferedFileOutputStreamIR(
                            compressedLogDir.resolve(variableIRFilename), compressionLevel);
                } else {
                    variableIR = new StandardVariableBufferedFileOutputStreamIR(
                            compressedLogDir.resolve(variableIRFilename), compressionLevel);
                }
            }

            logtypeDictionaryKey = new LogtypeByteArrayViewDictionaryKey();
            if (useCompactVariableEncoding) {
                variableDictionaryKey = new CompactVariableByteArrayViewDictionaryKey();
            } else {
                variableDictionaryKey = new StandardVariableByteArrayViewDictionaryKey();
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    @PluginFactory
    public static CompressedLogFileV4 createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter,
            @PluginAttribute("fileName") final String fileName,
            @PluginAttribute(value = "bufferSize", defaultInt = 64 * 1024 * 1024) int bufferSize,   // use 64MB by default
            @PluginAttribute(value = "compressionLevel", defaultInt = 0) int compressionLevel,
            @PluginAttribute(value = "useCompactVariableEncoding", defaultBoolean = true) boolean useCompactVariableEncoding,
            @PluginAttribute(value = "useMemoryMappedIO", defaultBoolean = true) boolean useMemoryMappedIO,
            @PluginAttribute(value = "enableDebugOutput", defaultBoolean = false) boolean enableDebugOutput,
            @PluginAttribute(value = "ignoreExceptions", defaultBoolean = false) boolean ignoreExceptions
    ) {
        if (null == name) {
            LOGGER.error("No name provided for " + CompressedLogFileV4.class.getName());
            return null;
        }
        if (null == layout) {
            layout = PatternLayout.createDefaultLayout();
        }

        CompressedLogFilePatternLayoutContainer compressedLogPatternLayoutContainer =
                new CompressedLogFilePatternLayoutContainer((PatternLayout) layout);

        if (null == fileName) {
            LOGGER.error("No file name provided for " + CompressedLogFileV4.class.getName());
            return null;
        }

        return new CompressedLogFileV4(name, filter, compressedLogPatternLayoutContainer,
                fileName, bufferSize, compressionLevel, useCompactVariableEncoding,
                useMemoryMappedIO, enableDebugOutput, ignoreExceptions);
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        try {
            variableDict.close();
            logtypeDict.close();
            timestampIR.close();
            logtypeIR.close();
            variableIR.close();
            System.out.println("Gracefully Stopped " +  this.getName() + " appender");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
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
    public synchronized void append(LogEvent event) {
        // Using a global lock for now. Locks are to handle when multiple threads print log messages
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
            }

            if (enableDebugOutput) {
                System.out.write(logMessage.array(), 0, logMessage.position());
            }

            // Insert into timestamp column right away, no parsing required
            // Will encode the format into metadata in future implementation
            timestampIR.putTimestamp(event.getTimeMillis());
            final byte integerEncodedVariableDelim = 17;
            final byte floatEncodedVariableDelim = 19;
            final byte variableIdDelim = 18;
            while (parseNextVar(logMessage)) {
                variableDictionaryKey.wrap(logMessage.array(), tokenBounds.beginPos, tokenBounds.endPos);
                if (variableDictionaryKey.encodeAsIntegerType()) {
                    logtype.put(integerEncodedVariableDelim);
                    variableIR.putIntegerEncodedVariable(variableDictionaryKey);
                } else if (variableDictionaryKey.encodeAsFloatType()) {
                    logtype.put(floatEncodedVariableDelim);
                    variableIR.putDecimalEncodedVariable(variableDictionaryKey);
                } else {
                    logtype.put(variableIdDelim);
                    variableIR.putVariableID(variableDictionaryKey, variableDict);
                }
            }
            logtypeIR.putLogtype(logtypeDict.getCompactId(logtypeDictionaryKey.wrap(logtype)));
        } catch (Exception ex) {
            if (!ignoreExceptions()) {
                throw new AppenderLoggingException(ex);
            }
        }
    }
}
