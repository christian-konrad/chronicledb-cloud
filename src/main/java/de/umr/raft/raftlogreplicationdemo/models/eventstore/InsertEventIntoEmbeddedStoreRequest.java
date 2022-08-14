package de.umr.raft.raftlogreplicationdemo.models.eventstore;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import de.umr.event.Event;
import lombok.*;

import java.util.List;

// TODO have proto for this instead of json only
@Data
@NoArgsConstructor
@AllArgsConstructor
public class InsertEventIntoEmbeddedStoreRequest {

    @NonNull
    @JsonProperty
    @Getter
    private List<Event> events = List.of();

    @JsonPOJOBuilder
    public static class Builder {
    }
}