package com.example.replication.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "replica")
public class ReplicaProperties {
    private int n;
    private int w;
    private int r;
    private int delayMaxMs;
    private String myAddress;
    private String allAddresses; // comma-separated

    public List<String> getAllAddressesList() {
        if (allAddresses == null || allAddresses.isBlank()) return List.of();
        return List.of(allAddresses.split(","));
    }
}
