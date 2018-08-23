package com.learning.logParser;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LoggedLine implements WritableComparable<LoggedLine> {
    private TimeStamp time;
    private Text type;
    private Text location;
    private Text message;

    public LoggedLine(TimeStamp time, Text type, Text location, Text message) {
        this.time = time;
        this.type = type;
        this.location = location;
        this.message = message;
    }

    public TimeStamp getTime() {
        return time;
    }

    public void setTime(TimeStamp time) {
        this.time = time;
    }

    public Text getType() {
        return type;
    }

    public void setType(Text type) {
        this.type = type;
    }

    public Text getLocation() {
        return location;
    }

    public void setLocation(Text location) {
        this.location = location;
    }

    public Text getMessage() {
        return message;
    }

    public void setMessage(Text message) {
        this.message = message;
    }

    @Override
    public int compareTo(LoggedLine line) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    }
}
