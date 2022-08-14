package de.umr.raft.raftlogreplicationdemo.models.sysinfo;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor(staticName = "of")
public class ClusterHealth {
    @Getter private final boolean isHealthy; // if consensus is achievable (> 50% nodes running), it is healthy
    @Getter private final List<NodeHealth> nodeHealths;
}
