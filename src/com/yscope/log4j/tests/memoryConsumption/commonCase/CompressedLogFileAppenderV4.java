package com.yscope.log4j.tests.memoryConsumption.commonCase;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

import java.util.concurrent.TimeUnit;

public class CompressedLogFileAppenderV4 {
    private static void initializeLog4j2(String appenderName) {
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setStatusLevel(Level.INFO);   // Hide log4j builder debug logs (too verbose)
        builder.setPackages("com.yscope.log4j.appenders.compressedLogFileAppender.v4");

        // create console appender
        AppenderComponentBuilder appenderBuilder = builder.newAppender(appenderName, "CompressedLogFileV4");
        appenderBuilder.add(builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%d{HH:mm:ss.SSS} %-5level - %msg%n"));
        appenderBuilder.addAttribute("fileName", "logs/throughputTests/" + appenderName + "/test.cla");
        appenderBuilder.addAttribute("bufferSize", 256 * 1024 * 1024);
        appenderBuilder.addAttribute("useCompactVariableEncoding", true);
        appenderBuilder.addAttribute("useMemoryMappedIO", false);
        appenderBuilder.addAttribute("enableDebugOutput", false);
        appenderBuilder.addAttribute("compressionLevel", 3);
        builder.add(appenderBuilder);

        // add root logger
        builder.add(builder.newRootLogger(Level.DEBUG).add(builder.newAppenderRef(appenderName)));
        LoggerContext ctx = Configurator.initialize(builder.build());
    }

    public static void main(String[] args) throws InterruptedException {
        String appenderName = "CompressedLogFileV4";
        initializeLog4j2(appenderName);

        final Logger logger = LogManager.getLogger(CompressedLogFileAppenderV4.class);

        int numLogMessages = 10000000;
//        System.gc();
//        TimeUnit.MILLISECONDS.sleep(5000);
        for (int i = 0; i < numLogMessages; i++) {
            logger.debug("This is a log message with one spark unique app id per line: " +
                    "app-20201119" + i % 1000 + "-" + i % 20000 + " from spark-node-" + i % 1000);
        }
//        TimeUnit.MILLISECONDS.sleep(3000);
//        System.gc();
//        TimeUnit.MILLISECONDS.sleep(2000);
        System.out.println("Test completed for " + numLogMessages + " log messages");
    }
}
