package com.raft.node.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record VoteResp(
        @JsonProperty("term") int term,
        @JsonProperty("voteGranted") boolean voteGranted
) {}