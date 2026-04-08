package com.raft.node.storage;

import org.springframework.stereotype.Component;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Component
public class StateMachine {
    private final Map<String, String> store = new ConcurrentHashMap<>();

    public void apply(String key, String value) {
        store.put(key, value);
    }

    public String get(String key) {
        return store.get(key);
    }
}