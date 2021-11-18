package com.yscope.log4j.tests.throughput;

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

public class GzipStreamingFileAppender {
    private static void initializeLog4j2(String appenderName) {
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setStatusLevel(Level.INFO);   // Hide log4j builder debug logs (too verbose)
        builder.setPackages("com.yscope.log4j.appenders.gzipStreamingFileAppender");

        // Create file appender
        AppenderComponentBuilder appenderBuilder = builder.newAppender(appenderName, "GzipStreamingFile");
        appenderBuilder.add(builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%d{HH:mm:ss.SSS} %-5level - %msg%n"));
        appenderBuilder.addAttribute("fileName", "logs/throughputTests/" + appenderName + "/test.gz");
        builder.add(appenderBuilder);

        // add root logger
        builder.add(builder.newRootLogger(Level.DEBUG).add(builder.newAppenderRef(appenderName)));
        LoggerContext ctx = Configurator.initialize(builder.build());
    }

    public static void main(String[] args) {
        String appenderName = "GzipStreamingFile";
        initializeLog4j2(appenderName);

        final Logger logger = LogManager.getLogger(GzipStreamingFileAppender.class);
        int numLogMessages = 100000;
        long start, end;
        start = System.nanoTime();
        for (int i = 0; i < numLogMessages; i++) {
            logger.debug("This is a standard log message with with one variable: " + i);
        }
        end = System.nanoTime();
        int nanoSecondsInSeconds = 1000 * 1000 * 1000;
        DecimalFormat decimalFormatter = new DecimalFormat("#,###");
        int eventPerSeconds = (int)((double) numLogMessages * nanoSecondsInSeconds/ (end - start));
        System.out.println("File appender: " + decimalFormatter.format(eventPerSeconds) + " msg/s");
    }
}
