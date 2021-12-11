package com.yscope.log4j.appenders.compressedLogFileAppender.v5.compressionDictionary;


import it.unimi.dsi.fastutil.Hash;

import java.util.Arrays;

public class Sha256DigestHashStrategy<K> implements Hash.Strategy<K> {
    @Override
    public int hashCode(Object o) {
        // Must be careful with sign extension when shifting
        // Leverage instruction level parallelism
        // Hashcode is simply the most significant 32 bytes
        byte[] digest = (byte[]) o;
        int a = (digest[0] & 0xFF) << 24;
        int b = (digest[1] & 0xFF) << 16;
        int c = (digest[2] & 0xFF) << 8;
        int d = digest[3] & 0xFF;
        int upper = a | b;
        int lower = c | d;
        return upper | lower;
    }

    @Override
    public boolean equals(Object a, Object b) {
        return Arrays.equals((byte[]) a, (byte[]) b);
    }
}