package com.yscope.log4j.tests.throughput;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.builder.api.*;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

import org.apache.logging.log4j.core.config.*;

import java.text.DecimalFormat;


public class CompressedRollingAppender {
    private static void initializeLog4j2(String appenderName) {
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setStatusLevel(Level.INFO);   // Hide log4j builder debug logs (too verbose)

        // Create file appender
        AppenderComponentBuilder appenderBuilder = builder.newAppender(appenderName, "RollingFile");
        appenderBuilder.add(builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%d{HH:mm:ss.SSS} %-5level - %msg%n"));
//        appenderBuilder.addAttribute("fileName", "logs/throughputTests/" + appenderName + "/test.log");
        appenderBuilder.addAttribute("filePattern", "logs/throughputTests/" + appenderName + "/test-%d{HH:mm:ss.SSS}.log.gz");

        ComponentBuilder triggerPolicy = builder.newComponent("Policies")
                .addComponent(builder.newComponent("SizeBasedTriggeringPolicy").addAttribute("size", "1M"));
        appenderBuilder.addComponent(triggerPolicy);
        ComponentBuilder directWriteRolloverStrategy = builder.newComponent("DirectWriteRolloverStrategy")
                .addAttribute("maxFiles", "10");
        appenderBuilder.addComponent(directWriteRolloverStrategy);

        builder.add(appenderBuilder);

        // add root logger
        builder.add(builder.newRootLogger(Level.DEBUG).add(builder.newAppenderRef(appenderName)));
        LoggerContext ctx = Configurator.initialize(builder.build());
    }

    public static void main(String[] args) {
        String appenderName = "CompressedRollingAppender";
        initializeLog4j2(appenderName);

        final Logger logger = LogManager.getLogger(CompressedRollingAppender.class);
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
        System.out.println("Compressed rolling file appender: " + decimalFormatter.format(eventPerSeconds) + " msg/s");
    }
}
