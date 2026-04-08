package com.raft.node.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record StatusDto(
        @JsonProperty("role") String role,
        @JsonProperty("term") int term,
        @JsonProperty("leader") String leader,
        @JsonProperty("commitIndex") int commitIndex,
        @JsonProperty("lastApplied") int lastApplied,
        @JsonProperty("logLength") int logLength
) {}