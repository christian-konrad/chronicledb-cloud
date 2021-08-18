package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.messages;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.util.Preconditions;

import static org.apache.ratis.util.ProtoUtils.toByteString;

public interface MetaDataChange {
    enum Type {
        SET, DELETE;

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

    class Utils {
        public static Message toMessage(final MetaDataChange e) {
            final byte[] buf = new byte[e.length()];
            final int length = e.toBytes(buf, 0);
            Preconditions.assertTrue(length == buf.length);
            return Message.valueOf(toByteString(buf), () -> "Message:" + e);
        }

        public static Message createMetaDataUpdate(String nodeId, String key, String value) {
            return nodeId == null || key == null ? NullValue.getInstance() : new MetaDataUpdate(nodeId, key, value);
        }

        public static Message createMetaDataDelete(String nodeId, String key) {
            return nodeId == null || key == null ? NullValue.getInstance() : new MetaDataDelete(nodeId, key);
        }

        public static MetaDataChange bytes2MetaDataChange(byte[] buf, int offset) {
            final Type type = Type.valueOf(buf[offset]);
            switch(type) {
                case NULL: return NullValue.getInstance();
                case SET: return new MetaDataUpdate(buf, offset);
                default:
                    throw new AssertionError("Unknown metadata change change type " + type);
            }
        }

        static byte[] toBytes(Type type, String nodeId, String payload) {
            final byte[] nodeIdBytes = nodeId.getBytes(MetaDataPropagateMessage.UTF8);
            final byte[] payloadBytes = payload.getBytes(MetaDataPropagateMessage.UTF8);
            final byte[] bytes = new byte[nodeIdBytes.length + payloadBytes.length + 2];
            bytes[0] = type.byteValue();
            bytes[1] = (byte) nodeIdBytes.length; // TODO limit nodeId to 32 bit
            // TODO
            System.arraycopy(nodeIdBytes, 0, bytes, 2, nodeIdBytes.length);
            System.arraycopy(payloadBytes, 0, bytes, 2 + nodeIdBytes.length, payloadBytes.length);
            return bytes;
        }
    }
}
