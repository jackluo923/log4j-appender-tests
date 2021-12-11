package com.yscope.log4j.tests.experimentations;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PerformanceTest {
    public static void main(String[] args) throws NoSuchAlgorithmException, DigestException {
        byte[] message = "this is a lo".getBytes(StandardCharsets.UTF_8);

        message[0] = 1;
        message[1] = 1;
        message[2] = 1;
        message[3] = 1;


        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] output = new byte[128];
        long start, end;
        int numEvents = 10000000;
        start = System.nanoTime();
        for (int i = 0; i < numEvents; i++) {
            digest.update(message);
            int numOutputBytes = digest.digest(output, 0, 32);
        }
        end = System.nanoTime();
        int nanoSecondsInSeconds = 1000 * 1000 * 1000;
        int eventPerSeconds = (int) ((double) numEvents * nanoSecondsInSeconds / (end - start));

        System.out.println("Number of events per second: " + eventPerSeconds);
    }
}
