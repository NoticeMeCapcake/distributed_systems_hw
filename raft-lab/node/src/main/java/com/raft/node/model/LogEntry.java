package com.raft.node.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record LogEntry(
        @JsonProperty("index") int index,
        @JsonProperty("term") int term,
        @JsonProperty("key") String key,
        @JsonProperty("value") String value
) {
    @JsonCreator
    public LogEntry {
    }
}