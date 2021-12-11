package com.yscope.log4j.appenders.compressedLogFileAppender.v2.binaryDataWrappers;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Base byteArray wrapper optimized for minimal memory overhead
 */
public class ByteArray {
    public final byte[] bytes;

    public ByteArray(byte[] bytes) {
        this.bytes = bytes;
        if (bytes.length <= 0) {
            throw new UnsupportedOperationException("Must wrap byteArray with length greater than 0");
        }
    }

    public int getViewSize() {
        return bytes.length;
    }

    @Override
    public String toString() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ByteArray otherByteArrayOrChildClass) {
            int viewSize = getViewSize();
            if (viewSize != otherByteArrayOrChildClass.getViewSize()) {
                return false;
            }
            if (other instanceof ByteArrayView otherByteArrayView) {
                // Comparing ByteArray with ByteArrayView (other)
                int otherByteArrayViewBeginIndex = otherByteArrayView.beginIndex;
                for (int i = 0; i < viewSize; i++) {
                    if (bytes[i] != otherByteArrayView.bytes[otherByteArrayViewBeginIndex + i]) {
                        return false;
                    }
                }
            } else {
                // Comparing ByteArray with ByteArray (other)
                return Arrays.equals(bytes, otherByteArrayOrChildClass.bytes);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        // Adapted from JDK's Arrays.hashCode() method
        int result = 1;
        for (byte aByte : bytes) {
            result = 31 * result + aByte;
        }
        return result;
    }
}
