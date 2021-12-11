package com.yscope.log4j.appenders.compressedLogAggregationAppender.v3;

import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.HashMapKey.CompactVariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.HashMapKey.LogtypeByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.HashMapKey.StandardVariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.HashMapKey.VariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.intermediateRepresentation.aggregation.BaseAggregationOutputStreamIR;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.intermediateRepresentation.aggregation.CompactAggregationOutputStreamIR;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.intermediateRepresentation.aggregation.StandardAggregationOutputStreamIR;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.utilityClasses.PatternLayoutBufferDestination;
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
import java.util.concurrent.TimeUnit;

/**
 * v3 - Experiment with log aggregation stream appender
 *  - adapted from compressedLogFileAppenderV3, deduplication
 *  - benchmark shows it's slower than v2, not really worth it at all
 */
@Plugin(name = "CompressedLogAggregationV3", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class CompressedLogAggregationV3 extends AbstractAppender {
    private final boolean useCompactVariableEncoding;
    private int compressionLevel;
    private Path compressedLogFile;

    private BaseAggregationOutputStreamIR aggregationOutputStreamIR;

    private boolean enableDebugOutput;

    private PatternLayoutBufferDestination patternLayoutBufferDestination;

    VariableByteArrayViewDictionaryKey variableDictionaryKey;
    LogtypeByteArrayViewDictionaryKey logtypeDictionaryKey;

    private ByteBuffer logtype;
    private TokenBounds tokenBounds;

    private CompressedLogAggregationV3(String name, Filter filter,
                                       CompressedLogFilePatternLayoutContainer compressedLogPatternLayoutContainer,
                                       String fileName, int compressionLevel, boolean useCompactVariableEncoding,
                                       boolean enableDebugOutput, final boolean ignoreExceptions) throws IOException {
        super(name, filter, compressedLogPatternLayoutContainer.getCompressedLogPatternLayout(), ignoreExceptions,
                Property.EMPTY_ARRAY);
        this.compressionLevel = compressionLevel;
        this.useCompactVariableEncoding = useCompactVariableEncoding;
        this.enableDebugOutput = enableDebugOutput;
        this.patternLayoutBufferDestination = new PatternLayoutBufferDestination(8192);

        logtype = ByteBuffer.allocate(8192);
        tokenBounds = new TokenBounds();

        // Create directory for log file if it doesn't exist
        if (compressionLevel != 0) {
            fileName += ".zst";
        }
        compressedLogFile = Path.of(fileName);
        Files.createDirectories(compressedLogFile.getParent());

        logtypeDictionaryKey = new LogtypeByteArrayViewDictionaryKey();
        if (useCompactVariableEncoding) {
            aggregationOutputStreamIR = new CompactAggregationOutputStreamIR(compressedLogFile, compressionLevel);
            variableDictionaryKey = new CompactVariableByteArrayViewDictionaryKey();
        } else {
            aggregationOutputStreamIR = new StandardAggregationOutputStreamIR(compressedLogFile, compressionLevel);
            variableDictionaryKey = new StandardVariableByteArrayViewDictionaryKey();
        }
    }

    @PluginFactory
    public static CompressedLogAggregationV3 createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter,
            @PluginAttribute("fileName") final String fileName,
            @PluginAttribute(value = "compressionLevel", defaultInt = 0) int compressionLevel,
            @PluginAttribute(value = "useCompactVariableEncoding", defaultBoolean = true) boolean useCompactVariableEncoding,
            @PluginAttribute(value = "enableDebugOutput", defaultBoolean = false) boolean enableDebugOutput,
            @PluginAttribute(value = "ignoreExceptions", defaultBoolean = false) boolean ignoreExceptions
    ) throws IOException {
        if (null == name) {
            LOGGER.error("No name provided for " + CompressedLogAggregationV3.class.getName());
            return null;
        }
        if (null == layout) {
            layout = PatternLayout.createDefaultLayout();
        }

        CompressedLogFilePatternLayoutContainer compressedLogPatternLayoutContainer =
                new CompressedLogFilePatternLayoutContainer((PatternLayout) layout);

        if (null == fileName) {
            LOGGER.error("No file name provided for " + CompressedLogAggregationV3.class.getName());
            return null;
        }

        return new CompressedLogAggregationV3(name, filter, compressedLogPatternLayoutContainer,
                fileName, compressionLevel, useCompactVariableEncoding, enableDebugOutput, ignoreExceptions);
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        try {
            aggregationOutputStreamIR.close();
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
            final byte integerEncodedVariableDelim = 17;
            final byte floatEncodedVariableDelim = 19;
            final byte variableIdDelim = 18;
            while (parseNextVar(logMessage)) {
                variableDictionaryKey.wrap(logMessage.array(), tokenBounds.beginPos, tokenBounds.endPos);
                if (variableDictionaryKey.encodeAsIntegerType()) {
                    logtype.put(integerEncodedVariableDelim);
                    aggregationOutputStreamIR.putEncodedVariable(variableDictionaryKey);
                } else if (variableDictionaryKey.encodeAsFloatType()) {
                    logtype.put(floatEncodedVariableDelim);
                    aggregationOutputStreamIR.putEncodedVariable(variableDictionaryKey);
                } else {
                    logtype.put(variableIdDelim);
                    aggregationOutputStreamIR.putDictionaryVariable(variableDictionaryKey);
                }
            }
            aggregationOutputStreamIR.putLogtype(logtypeDictionaryKey.wrap(logtype));
            aggregationOutputStreamIR.putTimestamp(event.getTimeMillis());
        } catch (Exception ex) {
            if (!ignoreExceptions()) {
                throw new AppenderLoggingException(ex);
            }
        }
    }
}
