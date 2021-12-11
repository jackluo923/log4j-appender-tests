package com.yscope.log4j.appenders.compressedLogAggregationAppender.v5;

import com.github.luben.zstd.ZstdInputStream;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v1.decompressionDictionary.BufferedDataOutputStreamDecompressionDictionary;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v1.decompressionDictionary.DecompressionDictionary;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v1.HashMapKey.CompactVariableByteArrayViewDictionaryKey;
import com.yscope.log4j.appenders.compressedLogAggregationAppender.v1.HashMapKey.LogtypeByteArrayViewDictionaryKey;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;

public class AggregationServer {
    private static DecompressionDictionary logtypeDict;
    private static DecompressionDictionary variableDict;
    private static byte[] digestBuffer = new byte[32];
    private static byte[] readBuffer = new byte[1024];

    private static DataOutputStream createOutputStream(Path path, int compressionLevel) throws IOException {
        BufferedOutputStream compressedInput = new BufferedOutputStream(Files.newOutputStream(path), 8192);
        ZstdCompressorOutputStream zstdCompressorOutputStream =
                new ZstdCompressorOutputStream(compressedInput, compressionLevel);
        BufferedOutputStream decompressedInput = new BufferedOutputStream(zstdCompressorOutputStream, 16384);
        return new DataOutputStream(decompressedInput);
    }

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        String fileName = "logs/throughputTests/CompressedLogAggregationV1/test.cla.10.zst";
        Path decompressedFile = Path.of(fileName);
        Path parent = decompressedFile.getParent();
        Files.createDirectories(parent);

        int compressionLevel = 3;

        logtypeDict = new BufferedDataOutputStreamDecompressionDictionary(
                Path.of(fileName + ".logtype.dict.zst"), compressionLevel);
        variableDict = new BufferedDataOutputStreamDecompressionDictionary(
                Path.of(fileName + ".variable.dict.zst"), compressionLevel);

        DataOutputStream timestamp = createOutputStream(Path.of(fileName + ".timestamp.bin.zst"), compressionLevel);
        DataOutputStream logtype = createOutputStream(Path.of(fileName + ".logtype.bin.zst"), compressionLevel);
        DataOutputStream variable = createOutputStream(Path.of(fileName + ".variable.bin.zst"), compressionLevel);

        DataInputStream compressedInputStream =
                new DataInputStream(new BufferedInputStream(new ZstdInputStream(new BufferedInputStream(Files.newInputStream(decompressedFile)))));

        CompactVariableByteArrayViewDictionaryKey variableKey = new CompactVariableByteArrayViewDictionaryKey();
        LogtypeByteArrayViewDictionaryKey logtypeKey = new LogtypeByteArrayViewDictionaryKey();

        try {
            while (true) {
                byte marker = compressedInputStream.readByte();
                if (marker == 0x0) {
                    break;
                }
                int operationType = marker >> 4;
                if (operationType == 0x1) {
                    // Timestamp
                    timestamp.writeLong(compressedInputStream.readLong());
                } else if (operationType == 0x2) {
                    // Logtype
                    int length = 32;
                    // Get sha256 digest first
                    byte digest_bytes_read = 0;
                    while (digest_bytes_read != length) {
                        digest_bytes_read += compressedInputStream.read(digestBuffer, digest_bytes_read, length - digest_bytes_read);
                    }

                    int lengthType = marker & 0xF;
                    if (lengthType == 0x0) {
                        // We know the sha256 is guaranteed to be in the dictionary
                        logtype.writeInt(logtypeDict.getExistingId(digestBuffer));
                    } else {
                        if (lengthType == 0x1) {
                            length = compressedInputStream.readByte();
                            byte bytes_read = 0;
                            while (bytes_read != length) {
                                bytes_read += compressedInputStream.read(readBuffer, bytes_read, length - bytes_read);
                            }
                        } else if (lengthType == 0x2) {
                            length = compressedInputStream.readShort();
                            short bytes_read = 0;
                            while (bytes_read != length) {
                                bytes_read += compressedInputStream.read(readBuffer, bytes_read, length - bytes_read);
                            }
                        } else {
                            length = compressedInputStream.readInt();
                            int bytes_read = 0;
                            while (bytes_read != length) {
                                bytes_read += compressedInputStream.read(readBuffer, bytes_read, length - bytes_read);
                            }
                        }
                        logtype.writeInt(logtypeDict.upsertId(digestBuffer, readBuffer, length));
                    }
                } else if (operationType == 0x3) {
                    // Dictionary Variable, assume compact
                    int length = 32;
                    // Get sha256 digest first
                    byte digest_bytes_read = 0;
                    while (digest_bytes_read != length) {
                        digest_bytes_read += compressedInputStream.read(digestBuffer, digest_bytes_read, length - digest_bytes_read);
                    }

                    int lengthType = marker & 0xF;
                    if (lengthType == 0x0) {
                        // We know the sha256 is guaranteed to be in the dictionary
                        variable.writeInt(variableDict.getExistingId(digestBuffer));
                    } else {
                        if (lengthType == 0x1) {
                            length = compressedInputStream.readByte();
                            byte bytes_read = 0;
                            while (bytes_read != length) {
                                bytes_read += compressedInputStream.read(readBuffer, bytes_read, length - bytes_read);
                            }
                        } else if (lengthType == 0x2) {
                            length = compressedInputStream.readShort();
                            short bytes_read = 0;
                            while (bytes_read != length) {
                                bytes_read += compressedInputStream.read(readBuffer, bytes_read, length - bytes_read);
                            }
                        } else {
                            length = compressedInputStream.readInt();
                            int bytes_read = 0;
                            while (bytes_read != length) {
                                bytes_read += compressedInputStream.read(readBuffer, bytes_read, length - bytes_read);
                            }
                        }
                        variable.writeInt(variableDict.upsertId(digestBuffer, readBuffer, length));
                    }
                } else if (operationType == 0x5) {
                    // Compact dictionary encoding
                    compressedInputStream.readInt();
                    variable.writeInt(compressedInputStream.readInt());
                }
            }
        } catch (EOFException ex) {
//            ex.printStackTrace();
            System.out.println("Done");
        }

        logtypeDict.close();
        variableDict.close();
        timestamp.close();
        logtype.close();
        variable.close();
    }
}
