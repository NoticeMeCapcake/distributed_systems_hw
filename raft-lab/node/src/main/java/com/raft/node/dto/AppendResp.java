package com.raft.node.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AppendResp(
        @JsonProperty("term") int term,
        @JsonProperty("success") boolean success
) {}