package com.yscope.log4j.tests.throughput;

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

public class CompressedLogFileAppenderV0 {
    private static void initializeLog4j2(String appenderName) {
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setStatusLevel(Level.INFO);   // Hide log4j builder debug logs (too verbose)
        builder.setPackages("com.yscope.log4j.appenders.compressedLogFileAppender.v0");

        // create console appender
        AppenderComponentBuilder appenderBuilder = builder.newAppender(appenderName, "CompressedLogFileV0");
        appenderBuilder.add(builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%d{HH:mm:ss.SSS} %-5level - %msg%n"));
        appenderBuilder.addAttribute("fileName", "logs/throughputTests/" + appenderName + "/test.cla");
        appenderBuilder.addAttribute("bufferSize", 64 * 1024 * 1024);
        appenderBuilder.addAttribute("enableCompression", true);
        appenderBuilder.addAttribute("compressionLevel", 21);
        builder.add(appenderBuilder);

        // add root logger
        builder.add(builder.newRootLogger(Level.DEBUG).add(builder.newAppenderRef(appenderName)));
        LoggerContext ctx = Configurator.initialize(builder.build());
    }

    public static void main(String[] args) {
        String appenderName = "CompressedLogFileV0";
        initializeLog4j2(appenderName);

        final Logger logger = LogManager.getLogger(CompressedLogFileAppenderV0.class);

        SparkLogParser sparkLogParser = new SparkLogParser(
                "/home/jack/yscope/performance-test/src/com/yscope/compression/test1/stderr_small",
                true);

        int numLogMessages = 100000;
        long start, end;
        start = System.nanoTime();
        for (Event event: sparkLogParser.getLogEvents()) {
            logger.debug(event.getMsg());
        }
        end = System.nanoTime();
        int nanoSecondsInSeconds = 1000 * 1000 * 1000;
        DecimalFormat decimalFormatter = new DecimalFormat("#,###");
        int eventPerSeconds = (int) ((double) numLogMessages * nanoSecondsInSeconds / (end - start));
        System.out.println(appenderName + ": " + decimalFormatter.format(eventPerSeconds) + " msg/s");
    }
}