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

public class ConsoleAppender {
    private static void initializeLog4j2(String appenderName) {
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setStatusLevel(Level.INFO);   // Hide log4j builder debug logs (too verbose)

        // create console appender
        AppenderComponentBuilder appenderBuilder = builder.newAppender(appenderName,  "CONSOLE")
                .addAttribute("target", org.apache.logging.log4j.core.appender.ConsoleAppender.Target.SYSTEM_OUT);
        appenderBuilder.add(builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%d{HH:mm:ss.SSS} %-5level - %msg%n"));
        builder.add(appenderBuilder);

        // add root logger
        builder.add(builder.newRootLogger(Level.DEBUG).add(builder.newAppenderRef(appenderName)));
        LoggerContext ctx = Configurator.initialize(builder.build());
    }

    public static void main(String[] args) {
        String appenderName = "Stdout";
        initializeLog4j2(appenderName);

        final Logger logger = LogManager.getLogger(ConsoleAppender.class);

        SparkLogParser sparkLogParser = new SparkLogParser(
                "logs/uncompressedSparkLogs/stderr_small",
                true);

        long start, end;
        start = System.nanoTime();
        for (Event event: sparkLogParser.getLogEvents()) {
            logger.debug(event.getMsg());
        }
        end = System.nanoTime();
        int nanoSecondsInSeconds = 1000 * 1000 * 1000;
        DecimalFormat decimalFormatter = new DecimalFormat("#,###");
        int eventPerSeconds = (int)((double) sparkLogParser.getLogEvents().size() * nanoSecondsInSeconds/ (end - start));
        logger.info("Console log appender: " + decimalFormatter.format(eventPerSeconds) + " msg/s");
    }
}
