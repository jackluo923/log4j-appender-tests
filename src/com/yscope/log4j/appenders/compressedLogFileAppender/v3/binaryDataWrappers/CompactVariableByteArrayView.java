package com.yscope.log4j.appenders.compressedLogFileAppender.v3.binaryDataWrappers;

import java.nio.ByteBuffer;

/**
 * Compact Variable ByteArray View
 */
public class CompactVariableByteArrayView extends ByteArrayView {
    public int signedIntBackedEncoding;

    public CompactVariableByteArrayView(ByteBuffer variable, int beginIndex, int endIndex) {
        super(variable.array(), beginIndex, endIndex);
    }

    public void setBounds(int beginIndex, int endIndex) {
        this.beginIndex = beginIndex;
        this.endIndex = endIndex;
    }

    /**
     * Parses the variable as an unsigned int if possible
     * Java rarely print unsigned int, conversion logic is partially adapted from JDK9's Int class
     * - Must be able to work with non-direct (heap-allocated) ByteBuffer containing string representation
     * - Must ensure start of value is an integer with no zero-padding or positive sign
     * - Must be able to parse int "in-place" without creating more garbage on the heap
     * Note: we always parse the representation as decimal (radix=10)
     * @return false on error, otherwise true
     */
    public boolean encodeAs32bitIntegerType() {
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

        int result = 0;
        int limit = -Integer.MAX_VALUE;
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
        signedIntBackedEncoding = negative ? result : -result;
        return true;
    }

    /**
     * Parses the variable as a custom 32bit decimal type if possible directly from byteArrayView,
     * then encode it as compression-friendly encoding inside an int primitive type.
     * Note that the range of this custom type support lossless encoding of decimal number
     * with significant decimal digits from 0 to 33,554,432. Decimal point is
     * free to float anywhere between first and last digit. This allows us to losslessly
     * encode the majority of printable floating point numbers. Some numbers will not be
     * able to be stored lossly such as (1/3) -> "0.3333333333333333", and we store these
     * type of variables in the dictionary.
     *
     * Parsing logic is adapted from C++ CLP parser and optimized to for zer-copy parsing
     * Encode into 32 bits with the following format (from MSB to LSB):
     * -  1 bit : is negative
     * - 25 bits: The digits of the double without the decimal, as an integer
     * -  3 bits: # of decimal digits minus 1
     *     - This format can represent doubles with between 1 and 8 decimal digits, so we use 4 bits and map the range [1, 8] to [0x0, 0x8]
     * -  3 bits: position of the decimal from the right minus 1
     *     - To see why the position is taken from the right, consider (1) "-1234567.8", (2) "-.12345678", and (3) ".12345678"
     *         - For (1), the decimal point is at index 8 from the left and index 1 from the right.
     *         - For (2), the decimal point is at index 1 from the left and index 8 from the right.
     *         - For (3), the decimal point is at index 0 from the left and index 8 from the right.
     *         - So if we take the decimal position from the left, it can range from 0 to 16 because of the negative sign. Whereas from the right, the
     *           negative sign is inconsequential.
     *     - Thus, we use 4 bits and map the range [1, 8] to [0x0, 0x8].
     *
     * @return false on error, otherwise true
     */
    public boolean encodeAs32BitDecimalType() {
        int pos = beginIndex;
        int MAX_DIGITS_IN_REPRESENTABLE_DOUBLE_VAR = 8;
        int maxLength = MAX_DIGITS_IN_REPRESENTABLE_DOUBLE_VAR + 1;   // +1 for decimal point

        // Check for a negative sign and assign negative bit to the intRepresentation
        signedIntBackedEncoding = 0;
        if ('-' == bytes[pos]) {
            signedIntBackedEncoding = -1;   // Set top bit to 1
            ++pos;
            // Include sign in max length
            ++maxLength;
        }

        // Check if value can be represented in encoded format
        if (this.getViewSize() > maxLength) {
            return false;
        }

        int numDigits = 0;
        int decimalPointPos = endIndex;
        int digits = 0;
        for (; pos < endIndex; ++pos) {
            byte c = bytes[pos];
            if ('0' <= c && c <= '9') {
                digits *= 10;
                digits += (c - '0');
                ++numDigits;
            } else if (endIndex == decimalPointPos && '.' == c) {
                decimalPointPos = pos;
            } else {
                // Invalid character or two decimal points detected
                return false;
            }
        }
        if (decimalPointPos == endIndex - 1) {
            // No decimal point found, decimal point is the last character (one before endIndex), or no digits found
            // Note: numDigit must be greater than 0 at this point, therefore there is no need to check numDigits
            return false;
        }
        if ((digits >> 25) > 0) {
            // We can only represent up to 25 of binary digits (up to 33,554,432)
            // This required because we cannot represent all numbers that are 8 decimal digits int
            return false;
        }

        // Encode remaining payload and or with the intRepresentation
        signedIntBackedEncoding |= (digits << 8) + ((numDigits - 1) << 3) + (decimalPointPos - beginIndex - 1);

        return true;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ByteArray otherLightweightByteBufferWrapper) {
            int viewSize = getViewSize();
            if (viewSize != otherLightweightByteBufferWrapper.getViewSize()) {
                return false;
            }
            if (other instanceof CompactVariableByteArrayView otherByteArrayWrapper) {
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
