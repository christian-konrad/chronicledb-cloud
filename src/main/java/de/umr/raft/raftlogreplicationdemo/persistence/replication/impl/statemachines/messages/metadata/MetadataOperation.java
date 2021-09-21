package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.messages.metadata;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.util.Preconditions;

import java.util.Map;

import static org.apache.ratis.util.ProtoUtils.toByteString;

public interface MetadataOperation {
    enum Type {
        NULL, SET, DELETE;

        byte byteValue() {
            return (byte) ordinal();
        }

        private static final Type[] VALUES = Type.values();

        static Type valueOf(byte b) {
            Preconditions.assertTrue(b >= 0);
            Preconditions.assertTrue(b < VALUES.length);
            return VALUES[b];
        }
    }

    int toBytes(byte[] buf, int offset);

    int length();

    void apply(Map<String, Map<String, String>> metadata);

    class Utils {
        public static Message toMessage(final MetadataOperation e) {
            final byte[] buf = new byte[e.length()];
            final int length = e.toBytes(buf, 0);
            Preconditions.assertTrue(length == buf.length);
            return Message.valueOf(toByteString(buf), () -> "Message:" + e);
        }

        public static MetadataOperation createMetaDataSetOperation(String nodeId, String key, String value) {
            return nodeId == null || key == null ? NullOperation.getInstance() : new MetadataSetOperation(nodeId, key, value);
        }

        public static MetadataOperation createMetaDataDeleteOperation(String nodeId, String key) {
            return nodeId == null || key == null ? NullOperation.getInstance() : new MetadataDeleteOperation(nodeId, key);
        }

        public static MetadataOperation bytes2MetaDataOperation(byte[] buf, int offset) {
            final Type type = Type.valueOf(buf[offset]);
            switch(type) {
                case NULL: return NullOperation.getInstance();
                case SET: return new MetadataSetOperation(buf, offset);
                case DELETE: return new MetadataDeleteOperation(buf, offset);
                default:
                    throw new AssertionError("Unknown metadata operation type " + type);
            }
        }
    }
}
