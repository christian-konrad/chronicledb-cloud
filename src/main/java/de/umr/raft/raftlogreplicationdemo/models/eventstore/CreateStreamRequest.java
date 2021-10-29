package de.umr.raft.raftlogreplicationdemo.models.eventstore;

import de.umr.event.schema.EventSchema;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CreateStreamRequest {
    @Getter @NonNull final String streamName;
    @Getter @NonNull final EventSchema schema;
}
