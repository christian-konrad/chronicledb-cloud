package de.umr.raft.raftlogreplicationdemo.models;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

@RequiredArgsConstructor(staticName = "of")
public class SimpleLogEntry {

    @Getter @NonNull private final long timestamp;
    @Getter @NonNull private final String content;

    public static SimpleLogEntry ofLogLine(String logLine) {
        val lineParts = logLine.split(" ");
        val isoTimestamp = lineParts[0];
        val timestamp = Instant.parse(isoTimestamp).toEpochMilli();
        val content = logLine.substring(isoTimestamp.length());
        return SimpleLogEntry.of(timestamp, content);
    }

    private String getFormattedTimestamp() {
        return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(timestamp));
    }

    @Override
    public String toString() {
        return getFormattedTimestamp() + " " + content + "\r\n";
    }

    public byte[] getBytes() {
        return this.toString().getBytes();
    }
}
