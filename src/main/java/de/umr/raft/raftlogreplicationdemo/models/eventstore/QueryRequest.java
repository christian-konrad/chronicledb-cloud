package de.umr.raft.raftlogreplicationdemo.models.eventstore;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@Builder(builderClassName = "Builder")
@AllArgsConstructor
@NoArgsConstructor
@JsonDeserialize(builder = QueryRequest.Builder.class )
public class QueryRequest {
    @NonNull
    @JsonProperty(required = true)
    private String queryString;

    @Default
    @JsonProperty
    private long startTime = Long.MIN_VALUE;

    @Default
    @JsonProperty
    private long endTime = Long.MAX_VALUE;

    public QueryRequest(String queryString) {
        this.queryString = queryString;
    }

    @JsonPOJOBuilder
    public static class Builder {
    }
}
