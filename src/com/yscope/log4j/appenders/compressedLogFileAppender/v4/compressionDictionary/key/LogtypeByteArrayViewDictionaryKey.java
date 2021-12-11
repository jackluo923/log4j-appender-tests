package com.yscope.log4j.appenders.compressedLogFileAppender.v4.compressionDictionary.key;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;

/**
 * This class should be instantiated preferably once, and wrap byte array and range afterwards
 * Warning: Only use ByteArrayViewDictionaryEntry as dictionary entries.
 *          Other use-case are not guaranteed to be correct
 */
public class LogtypeByteArrayViewDictionaryKey extends ByteArrayViewDictionaryKey {
    public LogtypeByteArrayViewDictionaryKey() throws NoSuchAlgorithmException {
        super();
    }

    public LogtypeByteArrayViewDictionaryKey(ByteBuffer variable, int beginIndex, int endIndex)
            throws NoSuchAlgorithmException, DigestException {
        super(variable.array(), beginIndex, endIndex);
    }

    public void setViewBounds(int beginIndex, int endIndex) throws DigestException {
        // We must invoke wrap again since the hash needs to be recomputed
        wrap(this.bytes, beginIndex, endIndex);
    }
}
