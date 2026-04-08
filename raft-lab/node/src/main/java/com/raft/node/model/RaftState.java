package com.raft.node.model;

import java.util.ArrayList;
import java.util.List;

public class RaftState {
    public int currentTerm = 0;
    public String votedFor = null;
    public List<LogEntry> log = new ArrayList<>();
}