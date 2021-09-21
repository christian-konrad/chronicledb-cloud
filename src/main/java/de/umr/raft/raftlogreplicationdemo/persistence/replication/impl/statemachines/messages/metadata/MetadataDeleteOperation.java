package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.messages.metadata;

import lombok.Getter;
import lombok.val;
import org.apache.ratis.util.Preconditions;

import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataDeleteOperation implements MetadataOperation {
    static final int NODE_ID_LENGTH_LIMIT = 32;
    static final int KEY_LENGTH_LIMIT = 32;

    // only support ascii chars ?
    // public static final Pattern PATTERN = Pattern.compile("[\\x00-\\x7F]*");

    static byte[] payloadToBytes(String nodeId, String key) {
        final byte[] nodeIdBytes = nodeId.getBytes(MetadataOperationMessage.UTF8);
        final byte[] keyBytes = key.getBytes(MetadataOperationMessage.UTF8);

        // we need 3 bytes to store type of operation and length of each part
        final byte[] bytes = new byte[nodeIdBytes.length + keyBytes.length + 3];

        if (nodeIdBytes.length > NODE_ID_LENGTH_LIMIT ||
            keyBytes.length > KEY_LENGTH_LIMIT) {
            // TODO specify clearly which part!
            throw new IllegalArgumentException("A part of the meta data operation exceeds the character limit");
        }

        // first bytes declare type of operation and length of each part
        bytes[0] = Type.DELETE.byteValue();
        bytes[1] = (byte) nodeIdBytes.length;
        bytes[2] = (byte) keyBytes.length;

        System.arraycopy(nodeIdBytes, 0, bytes, 3, nodeIdBytes.length);
        System.arraycopy(keyBytes, 0, bytes, 3 + nodeIdBytes.length, keyBytes.length);
        return bytes;
    }

    static String[] extractContent(byte[] buf, int offset) {
        Preconditions.assertTrue(buf[offset] == Type.DELETE.byteValue());
        final int nodeIdLength = buf[offset + 1];
        final int keyLength = buf[offset + 2];
        final byte[] nodeIdBytes = new byte[nodeIdLength];
        final byte[] keyBytes = new byte[keyLength];
        System.arraycopy(buf, offset + 3, nodeIdBytes, 0, nodeIdLength);
        System.arraycopy(buf, offset + 3 + nodeIdBytes.length, keyBytes, 0, keyLength);
        return new String[]{
                new String(nodeIdBytes, MetadataOperationMessage.UTF8),
                new String(keyBytes, MetadataOperationMessage.UTF8)
        };
    }

    static byte[] copyBytes(byte[] buf, int offset) {
        Preconditions.assertTrue(buf[offset] == Type.DELETE.byteValue());
        final int nodeIdLength = buf[offset + 1];
        final int keyLength = buf[offset + 2];
        final byte[] copy = new byte[nodeIdLength + keyLength + 3];
        System.arraycopy(buf, offset, copy, 0, copy.length);
        return copy;
    }

    @Getter private final String nodeId;
    @Getter private final String key;

    private final byte[] encoded;

    private MetadataDeleteOperation(String nodeId, String key, byte[] encoded) {
        this.nodeId = nodeId;
        this.key = key;
        this.encoded = encoded;

        // TODO need overall length limit?
        // TODO is it ok not to check size limits here but above in static deserializer method?
    }

    private MetadataDeleteOperation(String[] payload, byte[] encoded) {
        this(payload[0], payload[1], encoded);
    }

    public MetadataDeleteOperation(byte[] buf, int offset) {
        this(extractContent(buf, offset), copyBytes(buf, offset));
    }

    public MetadataDeleteOperation(String nodeId, String key) {
        this(nodeId, key, payloadToBytes(nodeId, key));
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
        if (!(obj instanceof MetadataDeleteOperation)) return false;

        final MetadataDeleteOperation that = (MetadataDeleteOperation) obj;
        return this.getNodeId().equals(that.getNodeId()) &&
                this.getKey().equals(that.getKey());
    }

    @Override
    public int hashCode() {
        return (nodeId + key).hashCode();
    }

    @Override
    public String toString() {
        return MessageFormat.format("{0}: DELETE {1}", nodeId, key);
    }

    @Override
    public void apply(Map<String, Map<String, String>> metadata) {
        val hasNodeMetadata = metadata.containsKey(nodeId);

        Map<String, String> nodeMetadata = metadata.containsKey(nodeId)
                ? metadata.get(nodeId)
                : new ConcurrentHashMap<>();

        nodeMetadata.remove(key);

        if (nodeMetadata.isEmpty()) {
            metadata.remove(nodeId);
        } else {
            metadata.put(nodeId, nodeMetadata);
        }
    }

}
