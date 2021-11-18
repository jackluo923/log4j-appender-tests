package com.yscope.log4j.appenders.zstdStreamingFileAppender;

import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Warning: Does not support manager reconfiguration
 */
@Plugin(name = "ZstdStreamingFile", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class ZstdStreamingFile extends AbstractAppender {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private OutputStream zstdOutputStream;

    private ZstdStreamingFile(String name, Filter filter, Layout<? extends Serializable> layout,
                              String fileName, final boolean ignoreExceptions) {
        super(name, filter, layout, ignoreExceptions);
        try {
            final Path logFile = Path.of(fileName);
            Files.createDirectories(logFile.getParent());
            zstdOutputStream = new ZstdCompressorOutputStream(Files.newOutputStream(Path.of(fileName)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PluginFactory
    public static ZstdStreamingFile createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter,
            @PluginAttribute("fileName") final String fileName,
            @PluginAttribute("ignoreExceptions") Boolean ignoreExceptions
    ) {
        if (name == null) {
            LOGGER.error("No name provided for CLPAppenderImpl");
            return null;
        }
        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }

        if (fileName == null) {
            LOGGER.error("No file name provided for ZstdStreamingFile");
            return null;
        }
        if (ignoreExceptions == null) {
            ignoreExceptions = true;
        }

        return new ZstdStreamingFile(name, filter, layout, fileName, ignoreExceptions);
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        try {
            zstdOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Gracefully Stopped " +  this.getName() + " appender");
        return true;
    }

    @Override
    public void append(LogEvent event) {
        // Using a global lock for now. Locks are to handle when multiple threads print log messages
        // Lock is only really needed in 2 conditions:
        // 1) when we insert into hash table and writing out values (via mmap)
        // 2) when we insert multiple variables into variable column
        try {
            readLock.lock();
            zstdOutputStream.write(getLayout().toByteArray(event));
        } catch (Exception ex) {
            if (!ignoreExceptions()) {
                throw new AppenderLoggingException(ex);
            }
        } finally {
            readLock.unlock();
        }
    }
}
