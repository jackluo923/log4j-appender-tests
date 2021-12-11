package com.yscope.log4j.tests.sparkLogs;

import com.yscope.logParser.Event;
import com.yscope.logParser.SparkLogParser;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

public class ZstdStreamingFileAppender {
    private static void initializeLog4j2(String appenderName) {
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setStatusLevel(Level.INFO);   // Hide log4j builder debug logs (too verbose)
        builder.setPackages("com.yscope.log4j.appenders.zstdStreamingFileAppender");

        // Create file appender
        AppenderComponentBuilder appenderBuilder = builder.newAppender(appenderName, "ZstdStreamingFile");
        appenderBuilder.add(builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%d{HH:mm:ss.SSS} %-5level - %msg%n"));
        appenderBuilder.addAttribute("fileName", "logs/sparkLogTest/" + appenderName + "/test.zst");
        builder.add(appenderBuilder);

        // add root logger
        builder.add(builder.newRootLogger(Level.DEBUG).add(builder.newAppenderRef(appenderName)));
        LoggerContext ctx = Configurator.initialize(builder.build());
    }

    public static void main(String[] args) throws InterruptedException {
        String appenderName = "ZstdStreamingFile";
        initializeLog4j2(appenderName);

        final Logger logger = LogManager.getLogger(ZstdStreamingFileAppender.class);

        SparkLogParser sparkLogParser = new SparkLogParser(
                "logs/uncompressedSparkLogs/stderr_very_small",
                true);

        long start, end;
        System.gc();
        start = System.nanoTime();
        for (Event event: sparkLogParser.getLogEvents()) {
            logger.debug(event.getMsg());
        }
        end = System.nanoTime();
        int nanoSecondsInSeconds = 1000 * 1000 * 1000;
        DecimalFormat decimalFormatter = new DecimalFormat("#,###");
        int eventPerSeconds = (int) ((double) sparkLogParser.getLogEvents().size() * nanoSecondsInSeconds / (end - start));
        System.out.println(appenderName + ": " + decimalFormatter.format(eventPerSeconds) + " msg/s");
    }
}
