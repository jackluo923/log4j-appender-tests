package com.yscope.logParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parse each log message into timestamp and log message portion
 * - Multi-line log message is combined into one log message
 * - Timestamp is parsed out separately using hard-coded spark log timetamp format
 *   - 21/10/11 06:15:01 INFO SecurityManager: Changing view acls groups to:
 *     - timestamp: <long>, msg: " INFO SecurityManager: Changing view acls groups to: "
 * Limitation: Only support parsing entire file at once. It should be fine for most cases since log files are small
 */
public class SparkLogParser {
    // Each log line must begin w/ timestamp, otherwise it's treated as a multiline log message
    final private String logRegex = "^(?<ts>^\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2})" +
            "(?<msg>[\\s\\S]+?)(?=(^\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2})|\\Z)";
    final private Pattern pattern = Pattern.compile(logRegex, Pattern.MULTILINE);
    final private DateFormat timestampFormatter = new SimpleDateFormat("yy/MM/dd hh:mm:ss");

    final private ArrayList<Event> logEvents = new ArrayList<>();

    public SparkLogParser(String sparkLogFilePath, Boolean dropMsgNewLine) {
        final String fileContent;
        try {
            fileContent = Files.readString(Path.of(sparkLogFilePath));
            final Matcher matcher = pattern.matcher(fileContent);
            while (matcher.find()) {
                Event logEvent = new Event(timestampFormatter.parse(matcher.group(1)).getTime(), matcher.group(2));
                if (dropMsgNewLine) {
                    logEvent.dropMsgNewLine();
                }
                logEvents.add(logEvent);
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Event> getLogEvents() {
        return logEvents;
    }

    public void printLogEvents () {
        for (Event logEvent : logEvents) {
            System.out.println(logEvent.toString().stripTrailing());
        }
    }
}
