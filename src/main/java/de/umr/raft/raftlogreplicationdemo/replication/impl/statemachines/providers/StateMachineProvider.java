package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.providers;

import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionName;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Holds a reference to a StateMachine and the corresponding RaftGroup configuration
 * for easy instantiation and passing of both
 */
public class StateMachineProvider<StateMachine extends BaseStateMachine> {

    @Getter @NonNull private final Constructor<? extends StateMachine> stateMachineConstructor;
    @Getter @NonNull private final StateMachineProvider.RaftGroupConfig raftGroupConfig;

    public StateMachineProvider(Class<? extends StateMachine> stateMachineImpl, StateMachineProvider.RaftGroupConfig raftGroupConfig) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        this.stateMachineConstructor = stateMachineImpl.getConstructor();
        this.raftGroupConfig = raftGroupConfig;
    }

    // TODO may pass config params here or in constructor
    public StateMachine createStateMachineInstance() throws InvocationTargetException, InstantiationException, IllegalAccessException {
        return stateMachineConstructor.newInstance();
    }

    public RaftGroup createRaftGroup(String serverName) {
        return RaftGroup.valueOf(raftGroupConfig.getRaftGroupId(serverName), raftGroupConfig.getPeers());
    }

    public PartitionInfo createPartitionInfo(String serverName) {
        // TODO check if is full qualified classname
        return PartitionInfo.of(raftGroupConfig.getPartitionName(serverName), createRaftGroup(serverName), stateMachineConstructor.getName());
    }

    public RaftGroupId getRaftGroupId(String serverName) {
        return raftGroupConfig.getRaftGroupId(serverName);
    }

    public String getStateMachineType() {
        return this.stateMachineConstructor.getName();
    }

    public List<String> getPeerIds() {
        return this.raftGroupConfig.peers.stream().map(raftPeer -> raftPeer.getId().toString()).collect(Collectors.toList());
    }

    @RequiredArgsConstructor(staticName = "of")
    public static class RaftGroupConfig {
        @Getter @NonNull private final List<RaftPeer> peers;
        @Getter @NonNull private final String groupName;

//        private RaftGroupConfig(@NonNull List<RaftPeer> peers, @NonNull String groupName) {
//            this.peers = peers;
//            this.groupName = groupName;
//        }

        public String getDisplayName(String serverName) {
            return String.format("%s:%s", serverName, groupName);
        }

        public PartitionName getPartitionName(String serverName) {
            return PartitionName.of(serverName, groupName);
        }

        public UUID getRaftGroupUUID(String serverName) {
            return UUID.nameUUIDFromBytes(getDisplayName(serverName).getBytes(StandardCharsets.UTF_8));
        }

        public RaftGroupId getRaftGroupId(String serverName) {
            UUID raftGroupUUID = getRaftGroupUUID(serverName);
            return RaftGroupId.valueOf(raftGroupUUID);
        }
    }
}
