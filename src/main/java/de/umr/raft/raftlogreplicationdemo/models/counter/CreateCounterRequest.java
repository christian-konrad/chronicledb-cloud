package de.umr.raft.raftlogreplicationdemo.models.counter;

import de.umr.raft.raftlogreplicationdemo.models.CreatePartitionRequest;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor()
public class CreateCounterRequest implements CreatePartitionRequest {

    @Getter @NonNull final String id;
    @Getter final long partitionsCount;
}
