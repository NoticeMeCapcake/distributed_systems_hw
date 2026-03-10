package com.example.clientservice.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "client")
public class ClientProperties {
    private String replicaAddresses;
    private int delayBetweenOpsMs = 100;

    public List<String> getReplicaAddressesList() {
        if (replicaAddresses == null || replicaAddresses.isBlank()) return List.of();
        return List.of(replicaAddresses.split(","));
    }
}