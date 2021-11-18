package com.yscope.logParser;

public class Event {
    private final long timestamp;
    private String msg;

    public Event(long timestamp, String msg) {
        this.timestamp = timestamp;
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }

    public void dropMsgNewLine() {
        msg.stripTrailing();
    }
    @Override
    public String toString() {
        return timestamp + " ->" + msg;
    }
}
