package com.yscope.IRAnalysis;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

public class IR_Format {
    // Log type and Variable Dictionaries
    private static final HashMap<ByteBuffer, Long> logTypeDictionary = new HashMap<>();
    private static final HashMap<ByteBuffer, Long> variableDictionary = new HashMap<>();
    private static Long maxLogTypeId = 0L;
    private static Long maxVarTypeId = 0L;

    static ByteArrayOutputStream ltDictBuffer = new ByteArrayOutputStream();   // format: <length><log type string>
    static ByteArrayOutputStream vtDictBuffer = new ByteArrayOutputStream();   // format: <length><variable type string>

    static ArrayList<Long> vt = new ArrayList<>();
    static ArrayList<Long> lt = new ArrayList<>();
    static ArrayList<Long> ts = new ArrayList<>();



    private static Long getLogTypeId(ByteBuffer logTypeStream) throws IOException {
        if (logTypeDictionary.containsKey(logTypeStream)) {
            return logTypeDictionary.get(logTypeStream);
        } else {
            // Generate new log type id and insert into log type dictionary
            maxLogTypeId += 1;
            logTypeDictionary.put(logTypeStream, maxLogTypeId);

            // Persist log type dictionary
            ltDictBuffer.write(logTypeStream.position());
            ltDictBuffer.write(logTypeStream.array());
//            ltDictBuffer.putInt(logTypeStream.position());
//            ltDictBuffer.put(logTypeStream.array());

            return maxLogTypeId;
        }
    }

    private static Long getVarTypeId(ByteBuffer var) throws IOException {
        if (variableDictionary.containsKey(var)) {
            return variableDictionary.get(var);
        } else {
            // Generate new var type id and insert into variable type dictionary
            maxVarTypeId += 1;
            variableDictionary.put(var, maxVarTypeId);

            // Persist variable type dictionary
            vtDictBuffer.write(var.position());
            vtDictBuffer.write(var.array());
//            vtDictBuffer.putInt(var.position());
//            vtDictBuffer.put(var.array());

            return maxVarTypeId;
        }
    }

    private static class TokenBounds {
        public int beginPos = 0;
        public int endPos = 0;
    }

    private static boolean isDelimiter(byte c) {
        return !('+' == c || ('-' <= c && c <= '9') || ('A' <= c && c <= 'Z') || '\\' == c || '_' == c || ('a' <= c && c <= 'z'));
    }

    private static boolean couldBeMultiDigitHexValue(byte[] str, int beginPos, int endPos) {
        if (endPos - beginPos < 2) {
            return false;
        }

        for (int i = beginPos; i < endPos; ++i) {
            byte c = str[i];
            if (!(('a' <= c && c <= 'f') || ('A' <= c && c <= 'F') || ('0' <= c && c <= '9'))) {
                return false;
            }
        }

        return true;
    }

    private static boolean getBoundsOfNextVar(byte[] msg, IR_Format.TokenBounds tokenBounds) {
        if (tokenBounds.endPos >= msg.length) {
            return false;
        }

        boolean isVar = false;
        while (!isVar) {
            tokenBounds.beginPos = tokenBounds.endPos;
            // Find next non-delimiter
            for (; tokenBounds.beginPos < msg.length; ++tokenBounds.beginPos) {
                byte c = msg[tokenBounds.beginPos];

                if (!isDelimiter(c)) {
                    break;
                }
            }
            if (msg.length == tokenBounds.beginPos) {
                // Early exit for performance
                return false;
            }

            boolean containsDecimalDigit = false;
            boolean containsAlphabet = false;

            // Find next delimiter
            tokenBounds.endPos = tokenBounds.beginPos;
            for (; tokenBounds.endPos < msg.length; ++tokenBounds.endPos) {
                byte c = msg[tokenBounds.endPos];
                if ('0' <= c && c <= '9') {
                    // Contains number, so treat token as a variable
                    containsDecimalDigit = true;
                } else if (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')) {
                    containsAlphabet = true;
                } else if (isDelimiter(c)) {
                    break;
                }
            }

            if (containsDecimalDigit ||
                    (tokenBounds.beginPos > 0 && '=' == msg[tokenBounds.beginPos - 1] && containsAlphabet) ||
                    couldBeMultiDigitHexValue(msg, tokenBounds.beginPos, tokenBounds.endPos)) {
                isVar = true;
            }
        }

        return true;
    }

    private static boolean parseNextVar(byte[] msg, IR_Format.TokenBounds tokenBounds,
                                        ByteArrayOutputStream logtypeStream, ByteArrayOutputStream varStream) {
        int lastVarEndPos = tokenBounds.endPos;
        if (getBoundsOfNextVar(msg, tokenBounds)) {
            logtypeStream.write(msg, lastVarEndPos, tokenBounds.beginPos - lastVarEndPos);

            varStream.reset();
            varStream.write(msg, tokenBounds.beginPos, tokenBounds.endPos - tokenBounds.beginPos);
            return true;
        }
        if (lastVarEndPos < msg.length) {
            logtypeStream.write(msg, lastVarEndPos, tokenBounds.beginPos - lastVarEndPos);
        }

        return false;
    }

    private static Long convertByteArrayToRepresentableIntegerVar(ByteBuffer var) {
        final byte[] bytes = var.array();
        if (bytes.length == 0) {
            // Empty string cannot be converted
            return null;
        }

        // Ensure start of value is an integer with no zero-padding or positive sign
        if ('-' == bytes[0]) {
            // Ensure first character after sign is a non-zero integer
            if (bytes.length < 2 || bytes[1] < '1' || '9' < bytes[1]) {
                return null;
            }
        } else {
            // Ensure first character is a digit
            if (bytes[0] < '0' || '9' < bytes[0]) {
                return null;
            }

            // Ensure value is not zero-padded
            if (bytes.length > 1 && '0' == bytes[0]) {
                return null;
            }
        }

        return convertByteArrayToLong(bytes);
    }

    private static Long convertByteArrayToLong(byte[] bytes) {
        try {
            return Long.parseLong(new String(bytes));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static long getCompressedSizeLT(byte[] buffer) throws IOException {
        String tempFile = "temp00.bin";
        DataOutputStream serializationStream = new DataOutputStream(new FileOutputStream(tempFile));
        serializationStream.write(buffer);
        serializationStream.close();
        Process process;
        try {
            process = Runtime.getRuntime().exec("rm " + tempFile + ".zst");
        } catch (IOException e) {
            e.printStackTrace();
        }
        process = Runtime.getRuntime().exec("zstd " + tempFile);
        return Files.size(Paths.get(tempFile + ".zst"));
    }

    public static long getCompressedSizeVT(byte[] buffer) throws IOException {
        String tempFile = "temp01.bin";
        DataOutputStream serializationStream = new DataOutputStream(new FileOutputStream(tempFile));
        serializationStream.write(buffer);
        serializationStream.close();
        Process process;
        try {
            process = Runtime.getRuntime().exec("rm " + tempFile + ".zst");
        } catch (IOException e) {
            e.printStackTrace();
        }
        process = Runtime.getRuntime().exec("zstd " + tempFile);
        return Files.size(Paths.get(tempFile + ".zst"));
    }

    public static long getCompressedTsColumnSize() throws IOException {
        String tempFile = "temp1.bin";
        DataOutputStream serializationStream = new DataOutputStream(new FileOutputStream(tempFile));
        for (Long aLong: ts) {
            serializationStream.writeLong(aLong);
        }
        serializationStream.close();
        Process process = null;
        try {
            process = Runtime.getRuntime().exec("rm " + tempFile + ".zst");
        } catch (IOException e) {
            e.printStackTrace();
        }
        process = Runtime.getRuntime().exec("zstd " + tempFile);
        return Files.size(Paths.get(tempFile + ".zst"));
    }

    public static long getCompressedLogTypeColumnSize() throws IOException {
        String tempFile = "temp2.bin";
        DataOutputStream serializationStream = new DataOutputStream(new FileOutputStream(tempFile));
        for (Long aLong: lt) {
            serializationStream.writeShort(Math.toIntExact(aLong));
        }
        serializationStream.close();
        Process process = null;
        try {
            process = Runtime.getRuntime().exec("rm " + tempFile + ".zst");
        } catch (IOException e) {
            e.printStackTrace();
        }
        process = Runtime.getRuntime().exec("zstd " + tempFile);
        return Files.size(Paths.get(tempFile + ".zst"));
    }

    public static long getCompressedVarColumnSize() throws IOException {
        String tempFile = "temp3.bin";
        DataOutputStream serializationStream = new DataOutputStream(new FileOutputStream(tempFile));
        for (Long aLong: vt) {
            serializationStream.writeLong(aLong);
        }
        serializationStream.close();
        Process process = Runtime.getRuntime().exec("rm " + tempFile + ".zst");
        process = Runtime.getRuntime().exec("zstd " + tempFile);
        return Files.size(Paths.get(tempFile + ".zst"));
    }

    public static void main(String[] args) throws IOException {
        DateFormat formatter = new SimpleDateFormat("yy/MM/dd hh:mm:ss");
        String inputFilePath = "src/com/yscope/test/compression/test1/stderr";
        long originalFileSize = Files.size(Paths.get(inputFilePath));
        System.out.println("Original file size: " + originalFileSize + " bytes");
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String line;

            while ((line = br.readLine()) != null) {
                long timestamp = formatter.parse(line.substring(0, 17)).getTime();
                ts.add(timestamp);

                byte[] logMessage = line.substring(17).getBytes(StandardCharsets.UTF_8);
                IR_Format.TokenBounds tokenBounds = new IR_Format.TokenBounds();
                ByteArrayOutputStream logTypeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream varStream = new ByteArrayOutputStream();

                while (parseNextVar(logMessage, tokenBounds, logTypeStream, varStream)) {
                    long encodedVar;
                    ByteBuffer var = ByteBuffer.wrap(varStream.toByteArray());
                    Long varAsLong = convertByteArrayToRepresentableIntegerVar(var);
                    if (null != varAsLong) {
                        // Encode as long
                        logTypeStream.write((char) 17);
                        encodedVar = varAsLong;
                    } else {
                        logTypeStream.write((char) 18);
                        encodedVar = getVarTypeId(var);
                    }

                    // variable type buffer
                    vt.add(encodedVar);
                }
                ByteBuffer logType = ByteBuffer.wrap(logTypeStream.toByteArray());
                lt.add(getLogTypeId(logType));
            }

            int ltDictSize = ltDictBuffer.toByteArray().length;
            int vtDictSize = vtDictBuffer.toByteArray().length;
            int tsSize = ts.size() * Long.BYTES;
            int logTypeShortSize = lt.size() * Short.BYTES;
            int logTypeIntSize = lt.size() * Integer.BYTES;
            int logTypeLongSize = lt.size() * Long.BYTES;
            int variableSize = vt.size() * Long.BYTES;

            int minUncompressedCLPIRSize = ltDictSize + vtDictSize + tsSize + logTypeShortSize + variableSize;
            int maxUncompressedCLPIRSize = ltDictSize + vtDictSize + tsSize + logTypeLongSize + variableSize;

            System.out.println("Minimum uncompressed CLP IR size: " + minUncompressedCLPIRSize + " bytes, " +
                    ((float)originalFileSize / minUncompressedCLPIRSize) + ":1 compression ratio");
            System.out.println("\tlogtype dictionary size: " + ltDictSize + "(" + ltDictSize * 100 / minUncompressedCLPIRSize + "%)");
            System.out.println("\tvariable dictionary size: " + vtDictSize + "(" + vtDictSize * 100 / minUncompressedCLPIRSize + "%)");
            System.out.println("\ttimestamp column size: " + tsSize + "(" + tsSize * 100 / minUncompressedCLPIRSize + "%)");
            System.out.println("\tlogType column sizes: " +
                    "short->" + logTypeShortSize + "(" + logTypeShortSize * 100 / minUncompressedCLPIRSize + "%), " +
                    "int->" + logTypeIntSize + "(" + logTypeIntSize * 100 / (minUncompressedCLPIRSize - logTypeShortSize + logTypeIntSize)  + "%), " +
                    "long->" + logTypeLongSize + "(" + logTypeLongSize * 100 / maxUncompressedCLPIRSize + "%), ");
            System.out.println("\tvariable column size: " + variableSize + "(" + variableSize * 100 / minUncompressedCLPIRSize + "%)");

            // Serialize and compress CLP IR files using zstd
            String serializedFileName = "serialized.bin";
            DataOutputStream serializationStream = new DataOutputStream(new FileOutputStream(serializedFileName));
            serializationStream.write(ltDictBuffer.toByteArray());
            serializationStream.write(vtDictBuffer.toByteArray());
            for (Long aLong: ts) {
                serializationStream.writeLong(aLong);
            }
            for (Long aLong : lt) {
                serializationStream.writeShort(Math.toIntExact(aLong));
//                serializationStream.writeLong(aLong);
            }
            for (Long aLong: vt) {
                serializationStream.writeLong(aLong);
            }
            serializationStream.close();
            Process process = Runtime.getRuntime().exec("rm " + serializedFileName + ".zst");
            process = Runtime.getRuntime().exec("zstd " + serializedFileName);
            long serializedAndCompressedSize = Files.size(Paths.get(serializedFileName + ".zst"));
            System.out.println("Serialized and compressed CLP IR size: " + serializedAndCompressedSize + " bytes, " +
                    (float) originalFileSize / serializedAndCompressedSize + ":1 compression ratio");
            long compressedLogTypeDictSize = getCompressedSizeLT(ltDictBuffer.toByteArray());
            long compressedVariableDictSize = getCompressedSizeVT(vtDictBuffer.toByteArray());
            long compressedTsColumnSize = getCompressedTsColumnSize();
            long compressedLogTypeColumnSize = getCompressedLogTypeColumnSize();
            long compressedVarColumnSize = getCompressedVarColumnSize();
            System.out.println("\tcompressed logType dictionary size: " + compressedLogTypeDictSize + "(" + compressedLogTypeDictSize*100/serializedAndCompressedSize + "%)");
            System.out.println("\tcompressed variable dictionary size: " + compressedVariableDictSize + "(" + compressedVariableDictSize*100/serializedAndCompressedSize + "%)");
            System.out.println("\tcompressed timestamp column size: " + compressedTsColumnSize + "(" + compressedTsColumnSize*100/serializedAndCompressedSize + "%)");
            System.out.println("\tcompressed log type column size: " + compressedLogTypeColumnSize + "(" + compressedLogTypeColumnSize*100/serializedAndCompressedSize + "%)");
            System.out.println("\tcompressed variable column size: " + compressedVarColumnSize + "(" + compressedVarColumnSize*100/serializedAndCompressedSize + "%)");

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

    }

}
