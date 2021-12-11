package com.yscope.log4j.appenders.compressedLogFileAppender.v1.binaryDataWrappers;

/**
 * ByteArray wrapper
 */
public class VariableByteArrayView extends ByteArrayView {
    public long longRepresentation;

    public VariableByteArrayView(byte[] bytes, int beginIndex, int endIndex) {
        super(bytes, beginIndex, endIndex);
    }

    /**
     * Parses the variable as a long if possible
     * Logic is partially adapted from JDK9's Long class
     * - Must be able to work with non-direct (heap-allocated) ByteBuffer containing string representation
     * - Must ensure start of value is an integer with no zero-padding or positive sign
     * - Must be able to parse long "in-place" without creating more garbage on the heap
     * Note: we always parse the representation as decimal (radix=10)
     * @return false on error, otherwise true
     */
    public boolean parseLong() {
        boolean negative = false;
        int i = beginIndex;

        if (i >= endIndex) {
            return false;
        }

        byte currByte = bytes[i];

        // Check for a negative sign
        if ('-' == currByte) {
            negative = true;
            currByte = bytes[++i];
        }
        // Integer can't be a lone "-" or ""
        if (i >= endIndex) {
            return false;
        }
        // If there is more than one digit, ensure value is not zero-padded
        if (i + 1 < endIndex && '0' == currByte) {
            return false;
        }

        long result = 0;
        long limit = -Long.MAX_VALUE;
        for (; i < endIndex; currByte = bytes[++i]) {
            // Accumulating negatively avoids surprises near MAX_VALUE
            int digit;
            if (currByte < '0' || '9' < currByte) {
                return false;
            }
            digit =  currByte - '0';

            result *= 10;
            if (result < limit + digit) {
                return false;
            }
            result -= digit;
        }
        longRepresentation = negative ? result : -result;
        return true;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ByteArray otherLightweightByteBufferWrapper) {
            int viewSize = getViewSize();
            if (viewSize != otherLightweightByteBufferWrapper.getViewSize()) {
                return false;
            }
            if (other instanceof VariableByteArrayView otherByteArrayWrapper) {
                int otherByteArrayWrapperBeginIndex = otherByteArrayWrapper.beginIndex;
                for (int i = 0; i < viewSize; i++) {
                    if (bytes[beginIndex + i] != otherByteArrayWrapper.bytes[otherByteArrayWrapperBeginIndex + i]) {
                        return false;
                    }
                }
            } else {
                for (int i = 0; i < viewSize; i++) {
                    if (bytes[beginIndex + i] != otherLightweightByteBufferWrapper.bytes[i]) {
                        return false;
                    }
                }
            }
        } else {
            return false;
        }
        return true;
    }
}
