package com.raft.node.controller;

import com.raft.node.dto.*;
import com.raft.node.service.RaftEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@Slf4j
public class RaftController {
    private final RaftEngine engine;

    public RaftController(RaftEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/raft/request-vote")
    public VoteResp requestVote(@RequestBody RequestVoteReq req) {
        log.info("Method requestVote, req: {}", req);

        return engine.handleRequestVote(req);
    }

    @PostMapping("/raft/append-entries")
    public AppendResp appendEntries(@RequestBody AppendEntriesReq req) {
        log.info("Method appendEntries, req: {}", req);

        return engine.handleAppendEntries(req);
    }

    @PostMapping("/command")
    public ResponseEntity<?> command(@RequestBody Map<String, String> body) {
        log.info("Method command, body: {}", body);

        String key = body.get("key");
        String value = body.get("value");
        CommandResp resp = engine.submitCommand(key, value);
        log.info("Method command, resp: {}", resp);


        if (resp.index() != null) {
            return ResponseEntity.ok(Map.of("index", resp.index()));
        } else if (resp.redirectUrl() != null) {
            return ResponseEntity.status(302).header("Location", resp.redirectUrl() + "/command").build();
        } else {
            return ResponseEntity.status(503).build();
        }
    }

    @GetMapping("/value/{key}")
    public ResponseEntity<Map<String, String>> getValue(@PathVariable String key) {
        log.info("Method getValue, key: {}", key);
        String val = engine.getValue(key);
        if (val == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(Map.of("value", val));
    }

    @GetMapping("/status")
    public StatusDto status() {
        log.info("Method status");
        return engine.getStatus();
    }
}