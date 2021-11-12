package com.yscope.test.log4j;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AppenderTester {
    static final Logger logger = LogManager.getLogger(AppenderTester.class);

    public static void main(String[] args) {
        int numLogMessages = 1000000;
        long start, end;
        start = System.nanoTime();
        for (int i = 0; i < numLogMessages; i++) {
            logger.debug("This is a standard log message with with one variable: " + i);
        }
        end = System.nanoTime();
        int nanoSecondsInSeconds = 1000 * 1000 * 1000;
        int eventPerSeconds = (int)((double) numLogMessages * nanoSecondsInSeconds/ (end - start));
        System.out.println("MVP implementation performance: " + eventPerSeconds + " msg/s");
    }
}
