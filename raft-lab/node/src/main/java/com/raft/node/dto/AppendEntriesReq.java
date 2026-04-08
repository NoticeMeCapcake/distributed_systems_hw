package com.raft.node.dto;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.raft.node.model.LogEntry;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public record AppendEntriesReq(
        @JsonProperty("term") int term,
        @JsonProperty("leaderId") String leaderId,
        @JsonProperty("prevLogIndex") int prevLogIndex,
        @JsonProperty("prevLogTerm") int prevLogTerm,
        @JsonProperty("entries") List<LogEntry> entries,
        @JsonProperty("leaderCommit") int leaderCommit
) {
}