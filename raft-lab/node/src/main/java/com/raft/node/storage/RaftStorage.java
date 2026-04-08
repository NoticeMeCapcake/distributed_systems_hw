package com.raft.node.storage;

import com.raft.node.model.RaftState;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class RaftStorage {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Path stateFile;
    private RaftState cachedState;

    public RaftStorage(com.raft.node.config.RaftProperties props) {
        this.stateFile = Path.of(props.dataDir(), "state.json");
        load();
    }

    public synchronized RaftState load() {
        try {
            if (Files.exists(stateFile)) {
                cachedState = mapper.readValue(stateFile.toFile(), RaftState.class);
            } else {
                cachedState = new RaftState();
                save(cachedState);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return cachedState;
    }

    public synchronized void save(RaftState state) {
        try {
            Files.createDirectories(stateFile.getParent());
            mapper.writeValue(stateFile.toFile(), state);
            this.cachedState = state;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized RaftState getState() {
        return cachedState;
    }
}