package com.raft.node.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.List;

@ConfigurationProperties(prefix = "raft")
public record RaftProperties(
        String nodeId,
        List<String> peers,
        int electionTimeoutMinMs,
        int electionTimeoutMaxMs,
        int heartbeatIntervalMs,
        String dataDir
) {}