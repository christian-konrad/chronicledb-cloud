package de.umr.raft.raftlogreplicationdemo.models.eventstore;

import de.umr.chronicledb.common.query.cursor.Cursor;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor
public class QueryResponse {
    @NonNull
    private final Cursor<? extends Event> events;
    @NonNull
    private final EventSchema eventSchema;
}
