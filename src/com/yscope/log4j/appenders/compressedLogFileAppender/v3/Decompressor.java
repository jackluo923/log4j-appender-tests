package com.yscope.log4j.appenders.compressedLogFileAppender.v3;

import com.yscope.log4j.appenders.compressedLogFileAppender.v3.utilityClasses.DecompressionDictionaryEntry;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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

    public static byte[] getBytesFrom64bitDecimalVariableEncoding(long decimalEncoding) {
        // Since java only offers signed number support except for char, we must be very careful
        // about parsing due to automatic sign extension, zero extension, truncation. Typically,
        // narrowing primitive conversion (long->int or long->byte) simply discards all but the
        // n lowest order bits. As a result, the sign of the input value may differ.
        // https://docs.oracle.com/javase/specs/jls/se7/html/jls-5.html

        // Cast decimalEncoding to int, mask, shift, +1 to fetch upper 4bits of unsigned byte (bit 4-7)
        int numDigits = ((((int)decimalEncoding) & 0xFF) >> 4) + 1;

        byte[] bytes;
        if (decimalEncoding < 0) {
            decimalEncoding &= Long.MAX_VALUE;   // Remove top negative bit
            bytes = new byte[numDigits + 2];  // + negative sign and decimal
        } else {
            bytes = new byte[numDigits + 1];
        }
        // Cast decimalEncoding to int, mask, +1 to fetch lower 4bits of unsigned byte (bit 0-3)
        int decimalPos = (((int)decimalEncoding) & 0x0F) + 1;
        long digits = decimalEncoding >> 8;   // convert representation to digits

        // Fill each byte of byteArray in reverse order
        for (int j = bytes.length - 1; j > decimalPos; --j) {
            // mod digits by 10, then cast '0' from char (16bit unsigned short) to 32bit signed integer,
            // add two operands together, truncate result down to a signed byte to get an ascii-digit
            bytes[j] = (byte) (digits % 10 + '0');
            digits /= 10;
        }
        // Fill decimal point
        bytes[decimalPos] = '.';
        // Fill the rest of the byteArray in reverse order until we run out of digits,
        // then we need to zero (the ascii-digit) fill them the rest of the array
        for(int j = decimalPos - 1; j >= 0; --j) {
            bytes[j] = (byte) (digits % 10 + '0');
            digits /= 10;
        }
        return bytes;
    }

    public static byte[] getBytesFrom32bitDecimalVariableEncoding(int decimalEncoding) {
        // Since java only offers signed number support except for char, we must be very careful
        // about parsing due to automatic sign extension, zero extension, truncation. Typically,
        // narrowing primitive conversion (long->int or long->byte) simply discards all but the
        // n lowest order bits. As a result, the sign of the input value may differ.
        // https://docs.oracle.com/javase/specs/jls/se7/html/jls-5.html

        // Cast decimalEncoding to int, mask, shift, +1 to compute numDigits
        int numDigits = ((((int)decimalEncoding) & 0x38) >> 3) + 1;

        byte[] bytes;
        if (decimalEncoding < 0) {
            decimalEncoding &= Integer.MAX_VALUE;   // Remove top negative bit
            bytes = new byte[numDigits + 2];  // + negative sign and decimal
        } else {
            bytes = new byte[numDigits + 1];
        }
        // Cast decimalEncoding to int, mask, +1 to compute decimalPos
        int decimalPos = (((int)decimalEncoding) & 0x3) + 1;
        long digits = decimalEncoding >> 6;   // convert representation to digits

        // Fill each byte of byteArray in reverse order
        for (int j = bytes.length - 1; j > decimalPos; --j) {
            // mod digits by 10, then cast '0' from char (16bit unsigned short) to 32bit signed integer,
            // add two operands together, truncate result down to a signed byte to get an ascii-digit
            bytes[j] = (byte) (digits % 10 + '0');
            digits /= 10;
        }
        // Fill decimal point
        bytes[decimalPos] = '.';
        // Fill the rest of the byteArray in reverse order until we run out of digits,
        // then we need to zero (the ascii-digit) fill them the rest of the array
        for(int j = decimalPos - 1; j >= 0; --j) {
            bytes[j] = (byte) (digits % 10 + '0');
            digits /= 10;
        }
        return bytes;
    }

    public static void decompress(Path decompressedLogFile,
                                  MappedByteBuffer tsBuf, MappedByteBuffer logtypeBuf, MappedByteBuffer varBuf) {
        // Create decompressed log file stream
        DateFormat dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS");
        System.out.println("Decompression started");
        try {
            BufferedOutputStream decompressedLogStream =
                    new BufferedOutputStream(new FileOutputStream(String.valueOf(decompressedLogFile)));

            byte[] digitBuf = new byte[19];   // 9,223,372,036,854,775,808 -> 19 digits
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
                        // Delimiter for unsigned long variable encoding
                        // Note: to be able to decompressed unsigned long representation of variable
                        // from signed long, manual parsing is necessary.
                        long unsignedLongEncoding = varBuf.getLong();
                        int j = digitBuf.length;
                        for (; unsignedLongEncoding != 0; --j) {
                            digitBuf[j] = (byte) (unsignedLongEncoding % 10);
                            unsignedLongEncoding /= 10;
                        }
                        j += 1;   // Increment offset to the first valid character
                        decompressedLogStream.write(digitBuf, j, digitBuf.length - j);
                    } else if (b == 18) {
                        // Delimiter for id variable type
                        decompressedLogStream.write(varDict.get((int) varBuf.getLong()).getBytes());
                    } else if (b == 19) {
                        // Note: if we use memory mapped file, we don't need to create a temporary byteArray
                        decompressedLogStream.write(getBytesFrom64bitDecimalVariableEncoding(varBuf.getLong()));
                    } else
                    {
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
        String appenderName = "CompressedLogFileV3";
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
