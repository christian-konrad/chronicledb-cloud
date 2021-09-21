package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.messages.metadata;

import lombok.Getter;
import lombok.val;
import org.apache.ratis.util.Preconditions;

import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataSetOperation implements MetadataOperation {
    public static final int NODE_ID_LENGTH_LIMIT = 32;
    public static final int KEY_LENGTH_LIMIT = 32;
    public static final int VALUE_LENGTH_LIMIT = 256;

    // only support ascii chars ?
    // public static final Pattern PATTERN = Pattern.compile("[\\x00-\\x7F]*");

    static byte[] payloadToBytes(String nodeId, String key, String value) {
        final byte[] nodeIdBytes = nodeId.getBytes(MetadataOperationMessage.UTF8);
        final byte[] keyBytes = key.getBytes(MetadataOperationMessage.UTF8);
        final byte[] valueBytes = value.getBytes(MetadataOperationMessage.UTF8);

        // we need 4 bytes to store type of operation and length of each part
        final byte[] bytes = new byte[nodeIdBytes.length + keyBytes.length + valueBytes.length + 4];

        if (nodeIdBytes.length > NODE_ID_LENGTH_LIMIT ||
            keyBytes.length > KEY_LENGTH_LIMIT ||
            valueBytes.length > VALUE_LENGTH_LIMIT) {
            // TODO specify clearly which part!
            throw new IllegalArgumentException("A part of the meta data operation exceeds the character limit");
        }

        // first bytes declare type of operation and length of each part
        bytes[0] = Type.SET.byteValue();
        bytes[1] = (byte) nodeIdBytes.length;
        bytes[2] = (byte) keyBytes.length;
        bytes[3] = (byte) valueBytes.length;

        System.arraycopy(nodeIdBytes, 0, bytes, 4, nodeIdBytes.length);
        System.arraycopy(keyBytes, 0, bytes, 4 + nodeIdBytes.length, keyBytes.length);
        System.arraycopy(valueBytes, 0, bytes, 4 + nodeIdBytes.length + keyBytes.length, valueBytes.length);
        return bytes;
    }

    static String[] extractContent(byte[] buf, int offset) {
        Preconditions.assertTrue(buf[offset] == Type.SET.byteValue());
        final int nodeIdLength = buf[offset + 1];
        final int keyLength = buf[offset + 2];
        final int valueLength = Byte.toUnsignedInt(buf[offset + 3]);
        final byte[] nodeIdBytes = new byte[nodeIdLength];
        final byte[] keyBytes = new byte[keyLength];
        final byte[] valueBytes = new byte[valueLength];
        System.arraycopy(buf, offset + 4, nodeIdBytes, 0, nodeIdLength);
        System.arraycopy(buf, offset + 4 + nodeIdBytes.length, keyBytes, 0, keyLength);
        System.arraycopy(buf, offset + 4 + nodeIdBytes.length + keyBytes.length, valueBytes, 0, valueLength);
        return new String[]{
                new String(nodeIdBytes, MetadataOperationMessage.UTF8),
                new String(keyBytes, MetadataOperationMessage.UTF8),
                new String(valueBytes, MetadataOperationMessage.UTF8)
        };
    }

    static byte[] copyBytes(byte[] buf, int offset) {
        Preconditions.assertTrue(buf[offset] == Type.SET.byteValue());
        final int nodeIdLength = buf[offset + 1];
        final int keyLength = buf[offset + 2];
        final int valueLength = Byte.toUnsignedInt(buf[offset + 3]);
        final byte[] copy = new byte[nodeIdLength + keyLength + valueLength + 4];
        System.arraycopy(buf, offset, copy, 0, copy.length);
        return copy;
    }

    @Getter private final String nodeId;
    @Getter private final String key;
    @Getter private final String value;

    private final byte[] encoded;

    private MetadataSetOperation(String nodeId, String key, String value, byte[] encoded) {
        this.nodeId = nodeId;
        this.key = key;
        this.value = value;
        this.encoded = encoded;

        // TODO need overall length limit?
        // TODO is it ok not to check size limits here but above in static deserializer method?
    }

    private MetadataSetOperation(String[] payload, byte[] encoded) {
        this(payload[0], payload[1], payload[2], encoded);
    }

    public MetadataSetOperation(byte[] buf, int offset) {
        this(extractContent(buf, offset), copyBytes(buf, offset));
    }

    public MetadataSetOperation(String nodeId, String key, String value) {
        this(nodeId, key, value, payloadToBytes(nodeId, key, value));
    }

    @Override
    public int toBytes(byte[] buf, int offset) {
        System.arraycopy(encoded, 0, buf, offset, encoded.length);
        return encoded.length;
    }

    @Override
    public int length() {
        return encoded.length;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof MetadataSetOperation)) return false;

        final MetadataSetOperation that = (MetadataSetOperation) obj;
        return this.getNodeId().equals(that.getNodeId()) &&
                this.getKey().equals(that.getKey()) &&
                this.getValue().equals(that.getValue());
    }

    @Override
    public int hashCode() {
        return (nodeId + key + value).hashCode();
    }

    @Override
    public String toString() {
        return MessageFormat.format("{0}: {1}={2}", nodeId, key, value);
    }

    @Override
    public void apply(Map<String, Map<String, String>> metadata) {
        val hasNodeMetadata = metadata.containsKey(nodeId);

        Map<String, String> nodeMetadata = metadata.containsKey(nodeId)
                ? metadata.get(nodeId)
                : new ConcurrentHashMap<>();

        if (value == null) {
            nodeMetadata.remove(key);
        } else {
            nodeMetadata.put(key, value);
        }

        if (nodeMetadata.isEmpty()) {
            metadata.remove(nodeId);
        } else {
            metadata.put(nodeId, nodeMetadata);
        }
    }
}
