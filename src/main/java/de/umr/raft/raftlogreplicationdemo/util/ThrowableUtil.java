package de.umr.raft.raftlogreplicationdemo.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public final class ThrowableUtil {
    public static byte[] getThrowableStackTraceBytes(Throwable throwable) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        throwable.printStackTrace(new PrintStream(baos));
        return baos.toByteArray();
    }
}
