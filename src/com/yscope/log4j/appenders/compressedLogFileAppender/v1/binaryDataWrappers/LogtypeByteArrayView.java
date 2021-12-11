package com.yscope.log4j.appenders.compressedLogFileAppender.v1.binaryDataWrappers;

import java.nio.ByteBuffer;

/**
 * Logtype ByteArray wrapper is created because the constructor takes in ByteBuffer instead of byteArray
 */
public class LogtypeByteArrayView extends ByteArrayView {
    public LogtypeByteArrayView(ByteBuffer logtype) {
        super(logtype.array(), 0, logtype.position());   // Only wrap valid region
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ByteArray otherLightweightByteBufferWrapper) {
            int viewSize = getViewSize();
            if (viewSize != otherLightweightByteBufferWrapper.getViewSize()) {
                return false;
            }
            if (other instanceof LogtypeByteArrayView otherByteArrayWrapper) {
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
