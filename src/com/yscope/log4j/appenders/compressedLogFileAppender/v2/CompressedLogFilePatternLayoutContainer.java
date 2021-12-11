package com.yscope.log4j.appenders.compressedLogFileAppender.v2;

import org.apache.logging.log4j.core.layout.PatternLayout;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compressed layout strips the timestamp form the layout and extracts the timestamp pattern
 * During decompression, the timestamp string will be re-generated using unix timestamp
 * Warning: Only supports logs with timestamp for now. Otherwise we error out immediately
 * Limitation: Currently assume timestamp is always in the beginning.
 *             Future CLP implementation will detect a "timestamp escape character" rather than hardcode
 *             the timestamp position to the beginning of the log message
 */
public class CompressedLogFilePatternLayoutContainer {
    private final String originalPatternLayoutStr;
    private final PatternLayout compressedLogPatternLayout;
    private final String timestampPattern;

    public CompressedLogFilePatternLayoutContainer(PatternLayout patternLayout) {
        // Parse the timestamp out into 3 parts: before tsPattern (prefix), tsPattern, afterTsPattern (suffix)
        // In the future we may inject a literal timestamp mark here to signify the position of timestamp
        this.originalPatternLayoutStr = patternLayout.getConversionPattern();
        final String timestampExtractionRegex = "(?<prefix>.*)%d\\{(?<timestampPattern>\\S+)\\}(?<suffix>.*)";
        final Pattern pattern = Pattern.compile(timestampExtractionRegex, Pattern.MULTILINE);
        final Matcher matcher = pattern.matcher(originalPatternLayoutStr);
        if (matcher.find()) {
            String transformedPatternLayoutString = matcher.group("prefix") + matcher.group("suffix");
            compressedLogPatternLayout = PatternLayout.newBuilder().withPattern(transformedPatternLayoutString).build();
            timestampPattern = matcher.group("timestampPattern");
        } else {
            throw new UnsupportedOperationException("Pattern layout must contain timestamp converter");
        }
    }

    public PatternLayout getOriginalPatternLayout() {
        return PatternLayout.newBuilder().withPattern(this.originalPatternLayoutStr).build();
    }

    public PatternLayout getCompressedLogPatternLayout() {
        return compressedLogPatternLayout;
    }

    public String getTimestampPattern() {
        return timestampPattern;
    }
}
