package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.metadata;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationProto;
import lombok.Getter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.ratis.util.ProtoUtils.toByteString;

@Deprecated
public class LegacyMetadataOperationMessage implements Message {
    public static final Charset UTF8 = StandardCharsets.UTF_8;

    @Getter private final MetadataOperation metaDataOperation;


    public LegacyMetadataOperationMessage(MetadataOperation metaDataOperation) {
        this.metaDataOperation = metaDataOperation;
    }

    public LegacyMetadataOperationMessage(byte[] buf, int offset) {
        metaDataOperation = MetadataOperation.Factory.bytes2MetaDataOperation(buf, offset);
    }

    public LegacyMetadataOperationMessage(ByteString bytes) {
        this(bytes.toByteArray(), 0);
    }

    @Override
    public ByteString getContent() {
        final int length = metaDataOperation.length();
        final byte[] bytes = new byte[length];
        metaDataOperation.toBytes(bytes, 0);
        return toByteString(bytes);
    }

    @Override
    public String toString() {
        return metaDataOperation.toString();
    }

    public void apply(Map<String, Map<String, String>> metadata) {
        metaDataOperation.apply(metadata); // TODO may return status info
    }
}
