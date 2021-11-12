package com.yscope.test.log4j;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Plugin(name = "CLPAppenderImpl", category = "Core", elementType = "appender", printObject = true)
public final class CLPAppenderImpl extends AbstractAppender {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();

    // Log type and Variable Dictionaries
    private final HashMap<ByteBuffer, Long> logTypeDictionary = new HashMap<>();
    private final HashMap<ByteBuffer, Long> variableDictionary = new HashMap<>();
    private Long maxLogTypeId = 0L;
    private Long maxVarTypeId = 0L;

    // Log type and variable type dictionary and the three data columns mmapped buffer
    MappedByteBuffer ltDictBuffer;   // format: <length><log type string>
    MappedByteBuffer vtDictBuffer;   // format: <length><variable type string>
    MappedByteBuffer tsBuffer;
    MappedByteBuffer ltBuffer;
    MappedByteBuffer vtBuffer;

    private CLPAppenderImpl(String name, Filter filter, Layout<? extends Serializable> layout,
                            final boolean ignoreExceptions) {
        super(name, filter, layout, ignoreExceptions);

        // Initialize memory mapped files
        int maxMappedFileSize = 64 * 1024 * 1024;   // 64MB
        String output_directory = "compressed_data";
        (new File(output_directory)).mkdir();
        Path ltDictPath = Path.of(output_directory + "/lt.dict.bin");
        Path vtDictPath = Path.of(output_directory + "/vt.dict.bin");
        Path tsPath = Path.of(output_directory + "/ts.bin");
        Path ltPath = Path.of(output_directory + "/lt.bin");
        Path vtPath = Path.of(output_directory + "/vt.bin");

        try {
            // Initialize memory mapped files
            FileChannel ltDictChannel = FileChannel.open(ltDictPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            FileChannel vtDictChannel = FileChannel.open(vtDictPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            FileChannel tsChannel = FileChannel.open(tsPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            FileChannel ltChannel = FileChannel.open(ltPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            FileChannel vtChannel = FileChannel.open(vtPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);

            ltDictBuffer = ltDictChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxMappedFileSize);
            vtDictBuffer = vtDictChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxMappedFileSize);
            tsBuffer = tsChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxMappedFileSize);
            ltBuffer = ltChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxMappedFileSize);
            vtBuffer = vtChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxMappedFileSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Long getLogTypeId(ByteBuffer logTypeStream) {
        if (logTypeDictionary.containsKey(logTypeStream)) {
            return logTypeDictionary.get(logTypeStream);
        } else {
            // Generate new log type id and insert into log type dictionary
            maxLogTypeId += 1;
            logTypeDictionary.put(logTypeStream, maxLogTypeId);

            // Persist log type dictionary
            ltDictBuffer.putInt(logTypeStream.position());
            ltDictBuffer.put(logTypeStream.array());

            return maxLogTypeId;
        }
    }

    private Long getVarTypeId(ByteBuffer var) {
        if (variableDictionary.containsKey(var)) {
            return variableDictionary.get(var);
        } else {
            // Generate new var type id and insert into variable type dictionary
            maxVarTypeId += 1;
            variableDictionary.put(var, maxVarTypeId);

            // Persist variable type dictionary
            vtDictBuffer.putInt(var.position());
            vtDictBuffer.put(var.array());

            return maxVarTypeId;
        }
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
            tsBuffer.putLong(event.getTimeMillis());

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
                    encodedVar = getVarTypeId(var);
                }
                vtBuffer.putLong(encodedVar);
            }
            ByteBuffer logType = ByteBuffer.wrap(logTypeStream.toByteArray());
            ltBuffer.putLong(getLogTypeId(logType));

//            // Print out log message
//            System.out.write(logMessage);
        } catch (Exception ex) {
            if (!ignoreExceptions()) {
                throw new AppenderLoggingException(ex);
            }
        } finally {
            readLock.unlock();
        }
    }

    @PluginFactory
    public static CLPAppenderImpl createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter,
            @PluginAttribute("otherAttribute") String otherAttribute) {
        if (name == null) {
            LOGGER.error("No name provided for CLPAppenderImpl");
            return null;
        }
        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }

        // TODO: modify layout to strip timestamp from pattern, store the pattern into metadata
        return new CLPAppenderImpl(name, filter, layout, true);
    }
}
