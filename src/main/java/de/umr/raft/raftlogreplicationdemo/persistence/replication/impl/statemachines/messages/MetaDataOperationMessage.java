package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.messages;

import lombok.Getter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.ratis.util.ProtoUtils.toByteString;

public class MetaDataPropagateMessage implements Message {
    public static final Charset UTF8 = StandardCharsets.UTF_8;

    @Getter private final MetaDataOperation metaDataOperation;

    public MetaDataPropagateMessage(MetaDataOperation metaDataOperation) {
        this.metaDataOperation = metaDataOperation;
    }

    public MetaDataPropagateMessage(byte[] buf, int offset) {
        metaDataOperation = MetaDataOperation.Utils.bytes2MetaDataOperation(buf, offset);
    }

    public MetaDataPropagateMessage(ByteString bytes) {
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
}
