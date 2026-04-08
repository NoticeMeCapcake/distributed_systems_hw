package com.raft.node.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CommandResp(
        @JsonProperty("index") Integer index,
        @JsonProperty("redirectUrl") String redirectUrl
) {}