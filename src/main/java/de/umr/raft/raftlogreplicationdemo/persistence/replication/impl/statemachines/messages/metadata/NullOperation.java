package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.messages.metadata;

import org.apache.ratis.util.Preconditions;

import java.util.Map;

public class NullOperation implements MetadataOperation {
    private static final NullOperation INSTANCE = new NullOperation();

    public static NullOperation getInstance() {
        return INSTANCE;
    }

    private NullOperation() { }

    @Override
    public int toBytes(byte[] buf, int offset) {
        Preconditions.assertTrue(offset + length() <= buf.length);
        buf[offset] = Type.NULL.byteValue();
        return length();
    }

    @Override
    public int length() {
        return 1;
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public void apply(Map<String, Map<String, String>> metadata) {
        // noop
    }
}
