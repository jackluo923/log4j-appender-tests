package com.yscope.log4j.appenders.compressedLogAggregationAppender.v3.HashMapKey;

import java.nio.ByteBuffer;

/**
 * This class should be instantiated preferably once, and wrap byte array and range afterwards
 * Warning: Only use ByteArrayViewDictionaryEntry as dictionary entries.
 *          Other use-case are not guaranteed to be correct
 */
public class LogtypeByteArrayViewDictionaryKey extends ByteArrayViewDictionaryKey {
    public LogtypeByteArrayViewDictionaryKey() {
        super();
    }

    public LogtypeByteArrayViewDictionaryKey(ByteBuffer variable, int beginIndex, int endIndex) {
        super(variable.array(), beginIndex, endIndex);
    }

    public void setViewBounds(int beginIndex, int endIndex) {
        // We must invoke wrap again since the hash needs to be recomputed
        wrap(this.bytes, beginIndex, endIndex);
    }
}
