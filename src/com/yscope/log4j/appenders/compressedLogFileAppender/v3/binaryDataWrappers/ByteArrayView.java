package com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Windowed ByteArray wrapper encapsulates byteArray as well as begin and end indexes
 */
public class ByteArrayView extends ByteArray {
    public int beginIndex;
    public int endIndex;

    public ByteArrayView(byte[] bytes, int beginIndex, int endIndex) {
        super(bytes);
        this.beginIndex = beginIndex;
        this.endIndex = endIndex;
    }

    public int getViewSize() {
        return endIndex - beginIndex;
    }

    public byte[] getByteArrayViewRegionBytesDeepCopy() {
        return Arrays.copyOfRange(bytes, beginIndex, endIndex);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ByteArray otherByteArrayOrChildClass) {
            int viewSize = getViewSize();
            if (viewSize != otherByteArrayOrChildClass.getViewSize()) {
                return false;
            }
            if (other instanceof ByteArrayView otherByteArrayView) {
                // Comparing ByteArrayView with ByteArrayView (other)
                int otherByteArrayWrapperBeginIndex = otherByteArrayView.beginIndex;
                for (int i = 0; i < viewSize; i++) {
                    if (bytes[beginIndex + i] != otherByteArrayView.bytes[otherByteArrayWrapperBeginIndex + i]) {
                        return false;
                    }
                }
            } else {
                // Comparing ByteArrayView with ByteArray (other)
                for (int i = 0; i < viewSize; i++) {
                    if (bytes[beginIndex + i] != otherByteArrayOrChildClass.bytes[i]) {
                        return false;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        // Adapted from JDK's Arrays.hashCode() method
        // Compute the hashcode for the byteArray view only
        int result = 1;
        for (int i = beginIndex; i < endIndex; i++) {
            result = 31 * result + bytes[i];
        }
        return result;
    }

    @Override
    public String toString() {
        return new String(bytes, beginIndex, endIndex - beginIndex, StandardCharsets.UTF_8);
    }
}
