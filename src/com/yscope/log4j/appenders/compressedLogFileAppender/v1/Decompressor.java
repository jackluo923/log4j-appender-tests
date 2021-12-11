package com.yscope.log4j.appenders.compressedLogFileAppender.v1;

import com.yscope.log4j.appenders.compressedLogFileAppender.v1.utilityClasses.DecompressionDictionaryEntry;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * Extremely basic implementation of decompression from CLP-IR format
 * Assumptions: hard-coded log timestamp format and location
 * Purpose: Studying ball-park memory efficiency and performance of decompression logic in JAVA
 */
public class Decompressor {
    private static final ArrayList<DecompressionDictionaryEntry> logtypeDict = new ArrayList<>();
    private static final ArrayList<DecompressionDictionaryEntry> varDict = new ArrayList<>();

    public static void decompress(Path decompressedLogFile,
                                  MappedByteBuffer tsBuf, MappedByteBuffer logtypeBuf, MappedByteBuffer varBuf) {
        // Create decompressed log file stream
        DateFormat dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS");
        System.out.println("Decompression started");
        try {
            BufferedOutputStream decompressedLogStream =
                    new BufferedOutputStream(new FileOutputStream(String.valueOf(decompressedLogFile)));
            for (int eventNum = 0; eventNum < logtypeBuf.limit() / Character.BYTES; eventNum++) {
                if (eventNum % 250000 == 0) {
                    System.out.println("Decompressed " + eventNum + " log messages");
                }

                // Re-generate timestamp (assume timestamp is always in the front for now)
                byte[] timestampBytes = dateFormatter.format(tsBuf.getLong()).getBytes(StandardCharsets.UTF_8);
                decompressedLogStream.write(timestampBytes);

                // Regenerate log message
                DecompressionDictionaryEntry logtype = logtypeDict.get((int) logtypeBuf.getChar());
                for (int i = 0; i < logtype.length; i++) {
                    byte b = logtype.getByte(i);
                    if (b == 17) {
                        // Delimiter for native numerical variable type
                        decompressedLogStream.write(Long.toString(varBuf.getLong()).getBytes(StandardCharsets.UTF_8));
                    } else if (b == 18) {
                        // Delimiter for id variable type
                        decompressedLogStream.write(varDict.get((int) varBuf.getLong()).getBytes());
                    } else {
                        decompressedLogStream.write(b);
                    }
                }
            }
            decompressedLogStream.close();
            System.out.println("Decompression of " + logtypeBuf.limit() / Character.BYTES + " log messages finished");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String appenderName = "CompressedLogFileV1";
        Path compressedLogFile = Path.of("logs/throughputTests/" + appenderName + "/test.cla");
        Path compressedLogDir = compressedLogFile.getParent();
        Path decompressedLogFile = compressedLogDir.resolve("test.txt");
        try {
            // Regenerate memory mapped views of dictionary entries
            // Log type and variable type dictionary and the three data columns mmapped buffer
            FileChannel logtypeDictFC = FileChannel.open(compressedLogDir.resolve("logtype.dict"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ);
            MappedByteBuffer logtypeDictBuf = logtypeDictFC.map(FileChannel.MapMode.READ_ONLY, 0, logtypeDictFC.size());
            logtypeDictBuf.load();
            while (logtypeDictBuf.hasRemaining()) {
                int entryLength = logtypeDictBuf.getChar();   // Entry length is encoded as 16bit unsigned int
                int bufPosition = logtypeDictBuf.position();
                logtypeDict.add(new DecompressionDictionaryEntry(logtypeDictBuf, bufPosition, entryLength));
                logtypeDictBuf.position(entryLength + bufPosition);
            }

            FileChannel varDictFC = FileChannel.open(compressedLogDir.resolve("var.dict"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ);
            MappedByteBuffer varDictBuf = varDictFC.map(FileChannel.MapMode.READ_ONLY, 0, varDictFC.size());
            varDictBuf.load();
            while (varDictBuf.hasRemaining()) {
                int entryLength = varDictBuf.getChar();   // Entry length is encoded as 16bit unsigned int
                int bufPosition = varDictBuf.position();
                varDict.add(new DecompressionDictionaryEntry(varDictBuf, bufPosition, entryLength));
                varDictBuf.position(entryLength + bufPosition);
            }

            // Map in log data entries
            FileChannel tsFC = FileChannel.open(compressedLogDir.resolve("ts.bin"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ);
            MappedByteBuffer tsBuf = tsFC.map(FileChannel.MapMode.READ_ONLY, 0, tsFC.size());
            FileChannel logtypeFC = FileChannel.open(compressedLogDir.resolve("logtype.bin"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ);
            MappedByteBuffer logtypeBuf = logtypeFC.map(FileChannel.MapMode.READ_ONLY, 0, logtypeFC.size());
            FileChannel varFC = FileChannel.open(compressedLogDir.resolve("var.bin"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ);
            MappedByteBuffer varBuf = varFC.map(FileChannel.MapMode.READ_ONLY, 0, varFC.size());

            // Perform decompression
            decompress(decompressedLogFile, tsBuf, logtypeBuf, varBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
