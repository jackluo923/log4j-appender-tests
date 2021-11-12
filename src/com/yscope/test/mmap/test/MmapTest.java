package com.yscope.test.mmap.test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class MmapTest {
    static List<Long> tsColumn = new ArrayList<>();
    static List<Long> ltColumn = new ArrayList<>();
    static List<List<Long>> vtColumn = new ArrayList<>();

    private static void generateData(Integer numLogMessages, Integer numVariablesPerLogMessage) {
        // Each log message for CLP IR format contains following
        // 1. timestamp - 64bit integer
        // 2. log type id - 64bit integer
        // 3. list of variable type ids - list of 64bit integers

        // We generate random ids and place them into the in-memory ts, lt, vt columns
        for (int i = 0; i < numLogMessages; i++) {
            tsColumn.add((long) i);
            ltColumn.add((long) i);
            List<Long> varIds = new ArrayList<>();
            for (int j = 0; j < numVariablesPerLogMessage; j++) {
                varIds.add((long) (numVariablesPerLogMessage * i + j));
            }
            vtColumn.add(varIds);
        }
    }

    private static void dataOutputStreamImpl(Integer numLogMessagesToBuffer) {
        try {
            // Initialize buffered writer w/ default buffer size
            (new File("dataOutputStreamImpl")).mkdir();
            DataOutputStream tsStream = new DataOutputStream(new FileOutputStream("dataOutputStreamImpl/ts.bin"));
            DataOutputStream ltStream = new DataOutputStream(new FileOutputStream("dataOutputStreamImpl/lt.bin"));
            DataOutputStream vtStream = new DataOutputStream(new FileOutputStream("dataOutputStreamImpl/vt.bin"));

            // Write to ids corresponding to each log msg and flush buffer every numLogMessagesToBuffer
            int stopCondition = tsColumn.size();
            for (int i = 0; i < stopCondition; i++) {
                // Flush buffer every numLogMessagesToBuffer log messages to enable content to be available to readers
                if (i % numLogMessagesToBuffer == 0) {
                    tsStream.flush();
                    ltStream.flush();
                    vtStream.flush();
                }
                // Write data to buffered readers
                tsStream.writeLong(tsColumn.get(i));
                ltStream.writeLong(ltColumn.get(i));
                for (Long varId : vtColumn.get(i)) {
                    vtStream.writeLong(varId);
                }
            }
            tsStream.close();
            ltStream.close();
            vtStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void mmapImpl(Integer maxMappedFileSize, boolean flush) {
        // mmap implementation does not use buffering
        (new File("mmapImpl")).mkdir();
        Path tsPath = Path.of("mmapImpl/ts.bin");
        Path ltPath = Path.of("mmapImpl/lt.bin");
        Path vtPath = Path.of("mmapImpl/vt.bin");

        try {
            // Initialize memory mapped files
            FileChannel tsChannel = FileChannel.open(tsPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            FileChannel ltChannel = FileChannel.open(ltPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            FileChannel vtChannel = FileChannel.open(vtPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);

            MappedByteBuffer tsBuffer = tsChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxMappedFileSize);
            MappedByteBuffer ltBuffer = ltChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxMappedFileSize);
            MappedByteBuffer vtBuffer = vtChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxMappedFileSize);

            // Write to ids corresponding to each log msg and flush buffer every numLogMessagesToBuffer
            int stopCondition = tsColumn.size();
            for (int i = 0; i < stopCondition; i++) {
                // Write data to buffered readers
                tsBuffer.putLong(tsColumn.get(i));
                ltBuffer.putLong(ltColumn.get(i));
                for (Long varId : vtColumn.get(i)) {
                    vtBuffer.putLong(varId);
                }
            }

            // Flush buffer content to disk, happens off logging critical path when log4j exits
            // Flushing is not required for the content to be visible to other the reader
            // Kernel buffer can be shared directly to allow read access
            if (flush) {
                tsBuffer.force();
                ltBuffer.force();
                vtBuffer.force();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int numLogMessages = 100000;
        int maxMappedFileSize = 64 * 1024 * 1024;   // 64MB
        int numLogMessagesToBuffer = 4;

        generateData(numLogMessages, 4);

        long start, end;

        start = System.nanoTime();
        dataOutputStreamImpl(numLogMessagesToBuffer);
        end = System.nanoTime();
        System.out.println((double) numLogMessages * 1000 * 1000 * 1000 / (end - start));
        System.out.println("DataOutputStream: " + (int)((double) numLogMessages * 1000 * 1000 * 1000 / (end - start)) + " msg/s");

        start = System.nanoTime();
        mmapImpl(maxMappedFileSize, false);
        end = System.nanoTime();
        System.out.println((double) numLogMessages * 1000 * 1000 * 1000 / (end - start));
        System.out.println("Memory Map w/o Flush: " + (int)((double) numLogMessages * 1000 * 1000 * 1000 / (end - start)) + " msg/ms");

        start = System.nanoTime();
        mmapImpl(maxMappedFileSize, true);
        end = System.nanoTime();
        System.out.println((double) numLogMessages * 1000 * 1000 * 1000 / (end - start));
        System.out.println("Memory Map w/ Flush: " + (int)((double) numLogMessages * 1000 * 1000 * 1000 / (end - start)) + " msg/ms");

    }
}
