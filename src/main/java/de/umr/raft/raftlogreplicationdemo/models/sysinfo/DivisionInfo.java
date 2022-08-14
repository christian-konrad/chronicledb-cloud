package de.umr.raft.raftlogreplicationdemo.models.sysinfo;

import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import lombok.*;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.util.concurrent.ExecutionException;

@RequiredArgsConstructor(staticName = "of") @Builder
public class DivisionInfo {
    @Getter @NonNull private final String nodeId;
    @Getter @NonNull private final String raftGroupId;
    @Getter @NonNull private final String memberId;
    @Getter @NonNull private final boolean isAlive;
    @Getter @NonNull private final long currentTerm;
    @Getter @NonNull private final long lastAppliedIndex;
    @Getter @NonNull private final String role;

    public static String createDivisionMemberId(String nodeId, String groupId) {
        return nodeId + "@" + groupId;
    }

    public static DivisionInfo of(String nodeId, String raftGroupId, ReplicatedMetadataMap divisionMetaDataMap) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val isAlive = Boolean.valueOf(divisionMetaDataMap.get("isAlive"));
        val currentTerm = Long.valueOf(divisionMetaDataMap.get("currentTerm"));
        val lastAppliedIndex = Long.valueOf(divisionMetaDataMap.get("lastAppliedIndex"));
        val role = divisionMetaDataMap.get("role");

        // TODO more params
        return new DivisionInfoBuilder()
                .isAlive(isAlive)
                .currentTerm(currentTerm)
                .lastAppliedIndex(lastAppliedIndex)
                .role(role)
                .nodeId(nodeId)
                .raftGroupId(raftGroupId)
                .memberId(createDivisionMemberId(nodeId, raftGroupId))
                .build();
    }
}
