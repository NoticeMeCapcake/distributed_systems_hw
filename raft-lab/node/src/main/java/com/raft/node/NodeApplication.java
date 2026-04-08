package com.raft.node;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import com.raft.node.config.RaftProperties;

@SpringBootApplication
@EnableConfigurationProperties(RaftProperties.class)
public class NodeApplication {
    public static void main(String[] args) {
        SpringApplication.run(NodeApplication.class, args);
    }
}