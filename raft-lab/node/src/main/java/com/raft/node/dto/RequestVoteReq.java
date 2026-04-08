package com.raft.node.dto;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public record RequestVoteReq(
        @JsonProperty("term") int term,
        @JsonProperty("candidateId") String candidateId,
        @JsonProperty("lastLogIndex") int lastLogIndex,
        @JsonProperty("lastLogTerm") int lastLogTerm
) {

}