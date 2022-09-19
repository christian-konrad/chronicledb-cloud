package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionName;
import de.umr.raft.raftlogreplicationdemo.replication.api.PeerState;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PartitionProvider implements Iterable<PartitionInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionProvider.class);

    private final RaftConfig raftConfig;
    private final Consumer<PartitionName> onPartitionRegistering;
    private final Consumer<PartitionName> onPartitionRegistered;
    private final Consumer<PartitionName> onPartitionDetached;

    public PartitionProvider(RaftConfig raftConfig, Consumer<PartitionName> onPartitionRegistering, Consumer<PartitionName> onPartitionRegistered, Consumer<PartitionName> onPartitionDetached) {
        this.raftConfig = raftConfig;

        this.onPartitionRegistering = onPartitionRegistering;
        this.onPartitionRegistered = onPartitionRegistered;
        this.onPartitionDetached = onPartitionDetached;
    }

    // TODO concurrent or normal?
    private final Map<PartitionName, PartitionInfo> partitions = new ConcurrentHashMap<>();

    public void update(PartitionInfo partitionInfo) {
        var partitionName = partitionInfo.getPartitionName();
        var previousState = partitions.get(partitionName);

        var newPartitionState = partitionInfo.getPartitionState();

        partitions.put(partitionName, partitionInfo);

        if (previousState == null || !previousState.getPartitionState().equals(newPartitionState)) {
            handlePartitionStateChanged(partitionName, newPartitionState);
        }

        LOG.info("Partitions: {}", partitions);
    }

    private void handlePartitionStateChanged(PartitionName partitionName, PartitionInfo.PartitionState partitionState) {
        switch (partitionState) {
            case REGISTERING:
                onPartitionRegistering.accept(partitionName);
                break;
            case REGISTERED:
                onPartitionRegistered.accept(partitionName);
                break;
            case DETACHED:
                onPartitionDetached.accept(partitionName);
        }
    }

    public void updatePartitionState(PartitionName partitionName, PartitionInfo.PartitionState partitionState) {
        var partitionInfo = partitions.get(partitionName);

        // state unchanged
        if (partitionInfo.getPartitionState().equals(partitionState)) return;

        // state changed
        handlePartitionStateChanged(partitionName, partitionState);
        partitionInfo.setPartitionState(partitionState);
    }

    public PartitionInfo get(PartitionName partitionName) {
        return Optional.ofNullable(partitions.get(partitionName))
                .orElseThrow(() -> new IllegalArgumentException("No known partition " + partitionName.toString()) );
    }

    public boolean contains(PartitionName partitionName) {
        return partitions.containsKey(partitionName);
    }

    PartitionInfo detach(PartitionName partitionName) {
        update(PartitionInfo.of(partitionName, RaftGroup.emptyGroup(), partitions.get(partitionName).getStateMachineClassname(), PartitionInfo.PartitionState.DETACHED));
        return partitions.get(partitionName);
    }

    public Set<Map.Entry<PartitionName, PartitionInfo>> entrySet() {
        return partitions.entrySet();
    }

    @Override
    public Iterator<PartitionInfo> iterator() {
        return partitions.values().iterator();
    }

    public Stream<PartitionInfo> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    public int size() {
        return partitions.size();
    }

    @Override
    public String toString() {
        return "{" +
            partitions.values().stream().map(PartitionInfo::toString).collect(Collectors.joining(", ")) +
                "}";
    }
}
