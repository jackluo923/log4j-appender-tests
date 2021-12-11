package com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

/**
 * This class should be instantiated preferably once, and wrap byte array and range afterwards
 * Warning: Only use ByteArrayViewDictionaryEntry as dictionary entries.
 *          Other use-case are not guaranteed to be correct
 */
public class StandardVariableByteArrayViewDictionaryKey extends VariableByteArrayViewDictionaryKey {
    private long signedLongBackedEncoding;

    public StandardVariableByteArrayViewDictionaryKey() throws NoSuchAlgorithmException {
        super();
    }

    public long getStandardVariableEncoding() {
        return signedLongBackedEncoding;
    }

    /**
     * Parses the variable as a 64bit integer if possible
     * Java rarely print unsigned long, conversion logic is partially adapted from JDK9's Long class
     * - Must be able to work with non-direct (heap-allocated) ByteBuffer containing string representation
     * - Must ensure start of value is an integer with no zero-padding or positive sign
     * - Must be able to parse long "in-place" without creating more garbage on the heap
     * Note: we always parse the representation as decimal (radix=10)
     * @return false on error, otherwise true
     */
    public boolean encodeAsIntegerType() {
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
        signedLongBackedEncoding = negative ? result : -result;
        return true;
    }

    /**
     * Parses the variable as a custom 64bit decimal type if possible directly from byteArrayView,
     * then encode it as compression-friendly encoding inside a long primitive type.
     * Note that the range of this custom type support lossless encoding of decimal number
     * with significant decimal digits from 0 to 9,007,199,254,740,992. Decimal point is
     * free to float anywhere between first and last digit. This allows us to losslessly
     * encode the majority of printable floating point numbers. Some numbers will not be
     * able to be stored lossly such as (1/3) -> "0.3333333333333333", and we store these
     * type of variables in the dictionary.
     *
     * Parsing logic is adapted from C++ CLP parser and optimized to for zer-copy parsing
     * Encode into 64 bits with the following format (from MSB to LSB):
     * -  1 bit : is negative
     * -  2 bit : unused
     * - 53 bits: The digits of the double without the decimal, as an integer
     * -  4 bits: # of decimal digits minus 1
     *     - This format can represent doubles with between 1 and 16 decimal digits, so we use 4 bits and map the range [1, 16] to [0x0, 0xF]
     * -  4 bits: position of the decimal from the right minus 1
     *     - To see why the position is taken from the right, consider (1) "-123456789012345.6", (2) "-.1234567890123456", and (3) ".1234567890123456"
     *         - For (1), the decimal point is at index 16 from the left and index 1 from the right.
     *         - For (2), the decimal point is at index 1 from the left and index 16 from the right.
     *         - For (3), the decimal point is at index 0 from the left and index 16 from the right.
     *         - So if we take the decimal position from the left, it can range from 0 to 16 because of the negative sign. Whereas from the right, the
     *           negative sign is inconsequential.
     *     - Thus, we use 4 bits and map the range [1, 16] to [0x0, 0xF].
     *
     * @return false on error, otherwise true
     */
    public boolean encodeAsFloatType() {
        int pos = beginIndex;
        int MAX_DIGITS_IN_REPRESENTABLE_DOUBLE_VAR = 16;
        int maxLength = MAX_DIGITS_IN_REPRESENTABLE_DOUBLE_VAR + 1;   // +1 for decimal point

        // Check for a negative sign and assign negative bit to the longRepresentation
        signedLongBackedEncoding = 0;
        if ('-' == bytes[pos]) {
            signedLongBackedEncoding = -1;   // Set top bit to 1
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
        long digits = 0;
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

        // Encode remaining payload and or with the longRepresentation
        signedLongBackedEncoding |= (digits << 8) + ((numDigits - 1L) << 4) + (decimalPointPos - beginIndex - 1);

        return true;
    }



}
