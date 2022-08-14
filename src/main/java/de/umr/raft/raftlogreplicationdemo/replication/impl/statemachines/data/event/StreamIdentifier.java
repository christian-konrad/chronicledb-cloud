package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.nio.file.Path;

@RequiredArgsConstructor(staticName = "of")
public class StreamIdentifier {
    @Getter @NonNull private Path basePath;
    @Getter @NonNull private String streamName;
}
