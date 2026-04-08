package com.raft.node.service;

import com.raft.node.config.RaftProperties;
import com.raft.node.dto.*;
import com.raft.node.model.LogEntry;
import com.raft.node.model.RaftState;
import com.raft.node.storage.RaftStorage;
import com.raft.node.storage.StateMachine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.restclient.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@Service
@Slf4j
public class RaftEngine {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final RaftProperties props;
    private final RaftStorage storage;
    private final StateMachine stateMachine;
    private final RestTemplate restTemplate = new RestTemplateBuilder()
            .defaultMessageConverters()
            .build();

    // Volatile state
    private Role role = Role.FOLLOWER;
    private int commitIndex = 0;
    private int lastApplied = 0;
    private String leaderId = null;
    private final Map<String, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();
//    private volatile boolean electionTimerRunning = false;

    // Timers
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;
    private final Random random = new Random(Instant.now().getNano());

    // Lock for state safety
    private final ReentrantLock lock = new ReentrantLock(true);
    private final ReentrantLock sendAsyncLock = new ReentrantLock(true);

    public RaftEngine(RaftProperties props, RaftStorage storage, StateMachine stateMachine) {
        this.props = props;
        this.storage = storage;
        this.stateMachine = stateMachine;

    }

    @PostConstruct
    public void init() {
        resetElectionTimer();
        // Heartbeat only for leader, handled in role switch
    }

    @PreDestroy
    public void destroy() {
        scheduler.shutdown();
    }

    private void resetElectionTimer() {
        lock.lock();
        try {
            // Отменяем предыдущий таймер
            if (electionTimer != null && !electionTimer.isDone()) {
                log.info("Canceled election timer");
                electionTimer.cancel(true); // false = не прерывать если выполняется
            }

            int timeout = props.electionTimeoutMinMs() +
                    random.nextInt(props.electionTimeoutMaxMs() - props.electionTimeoutMinMs());

//            electionTimerRunning = true;
            electionTimer = scheduler.schedule(() -> {
//                if (electionTimerRunning) {
                    electionTimeoutTask();
//                }
            }, timeout, TimeUnit.MILLISECONDS);

            log.info("Reset election timer for {} ms (node={})", timeout, props.nodeId());
        } finally {
            lock.unlock();
        }
    }

    private void startHeartbeatTimer() {
        if (heartbeatTimer != null) heartbeatTimer.cancel(true);
        heartbeatTimer = scheduler.scheduleAtFixedRate(this::heartbeatTask, props.heartbeatIntervalMs(), props.heartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void electionTimeoutTask() {
        lock.lock();
        try {
//            electionTimerRunning = false;

            if (role != Role.LEADER) {
                log.info("Election timeout expired, starting election (node={})", props.nodeId());
                startElection();
            }
        } finally {
            lock.unlock();
        }
    }

    private int getElectionTimeout() {
        return props.electionTimeoutMinMs() + random.nextInt(props.electionTimeoutMaxMs() - props.electionTimeoutMinMs());
    }

    private void startElection() {
        lock.lock();
        try {
            RaftState state = storage.getState();
            state.currentTerm++;
            state.votedFor = props.nodeId();
            storage.save(state);

            role = Role.CANDIDATE;
            leaderId = null;

            // Сбрасываем таймер (не запускаем новый сразу)
            if (electionTimer != null) electionTimer.cancel(true);

            var votes = new AtomicInteger(1);
            int lastLogIndex = state.log.size();
            int lastLogTerm = lastLogIndex > 0 ? state.log.get(lastLogIndex - 1).term() : 0;

            for (String peer : props.peers()) {
                CompletableFuture.runAsync(() -> {
                    var req = new RequestVoteReq(state.currentTerm, props.nodeId(), lastLogIndex, lastLogTerm);
                    try {
                        var resp = restTemplate.postForObject(peer + "/raft/request-vote", req, VoteResp.class);
                        if (resp != null && resp.voteGranted()) {
                            lock.lock();
                            try {
                                // ПРОВЕРЯЕМ что всё ещё кандидат и term не изменился
                                if (role == Role.CANDIDATE && storage.getState().currentTerm == resp.term()) {
                                    if (votes.incrementAndGet() > props.peers().size() / 2) {
                                        becomeLeader();
                                    }
                                }
                            } finally {
                                lock.unlock();
                            }
                        } else if (resp != null && resp.term() > state.currentTerm) {
                            lock.lock();
                            try {
                                becomeFollower(resp.term(), null);
                            } finally {
                                lock.unlock();
                            }
                        }
                    } catch (Exception e) {
                        // Сетевая ошибка — игнорируем
                    }
                });
            }

            // Перезапускаем таймер выборов
            resetElectionTimer();
        } finally {
            lock.unlock();
        }
    }

    private void becomeLeader() {
        lock.lock();
        try {
            role = Role.LEADER;
            leaderId = props.nodeId();

            // Останавливаем election timer
//            electionTimerRunning = false;
            if (electionTimer != null) {
                electionTimer.cancel(true);
            }

            // Инициализируем nextIndex/matchIndex
            int lastLogIndex = storage.getState().log.size();
            for (String peer : props.peers()) {
                nextIndex.put(peer, lastLogIndex + 1);
                matchIndex.put(peer, 0);
            }

            // Запускаем heartbeat
            startHeartbeatTimer();
            heartbeatTask(); // Немедленно отправляем heartbeat

            log.info("Became leader (term={}, node={})", storage.getState().currentTerm, props.nodeId());
        } finally {
            lock.unlock();
        }
    }

    private void heartbeatTask() {
        lock.lock();
        try {
            log.info("Heartbeat task begin");
            resetElectionTimer();
            if (role != Role.LEADER) return;
            RaftState state = storage.getState();
            for (String peer : props.peers()) {
                log.info("Heartbeat to peer={}", peer);
                sendAppendEntries(peer, state, new ArrayList<>());
            }
        } finally {
            log.info("Heartbeat end");
            lock.unlock();
        }
    }

    private void sendAppendEntries(String peer, RaftState state, List<LogEntry> newEntries) {
        if (role != Role.LEADER) return;

        Integer next = nextIndex.get(peer);
        if (next == null) {
            next = state.log.size() + 1;
            nextIndex.put(peer, next);
        }

        int prevLogIndex = next - 1;
        int prevLogTerm = 0;

        if (prevLogIndex > 0) {
            if (prevLogIndex > state.log.size()) {
                // Лог отстал, уменьшаем nextIndex
                nextIndex.put(peer, state.log.size());
                sendAppendEntries(peer, state, new ArrayList<>());
                return;
            }
            prevLogTerm = state.log.get(prevLogIndex - 1).term();
        }

        // Формируем список записей для отправки
        List<LogEntry> entriesToSend = new ArrayList<>();

        // Добавляем записи из лога начиная с next
        for (int i = prevLogIndex; i < state.log.size(); i++) {
            entriesToSend.add(state.log.get(i));
        }

        // НОВЫЕ записи уже должны быть в логе! Не добавляем их отдельно.
        // newEntries используется только для логирования/отладки

        var req = new AppendEntriesReq(
                state.currentTerm,
                props.nodeId(),
                prevLogIndex,
                prevLogTerm,
                entriesToSend,
                commitIndex
        );

        CompletableFuture.runAsync(() -> {
            try {
                var resp = restTemplate.postForObject(peer + "/raft/append-entries",
                        req,
                        AppendResp.class);
//                log.info("in async sendAppend before lock");
                sendAsyncLock.lock();
//                log.info("in async sendAppend lock acquired");
                try {
                    if (resp != null) {
                        if (resp.term() > state.currentTerm) {
                            becomeFollower(resp.term(), null);
                        } else if (resp.success()) {
                            if (!newEntries.isEmpty()) {
                                int match = prevLogIndex + req.entries().size();
                                matchIndex.put(peer, match);
                                nextIndex.put(peer, match + 1);
                                updateCommitIndex(state);
                            }
                        } else {
                            // Decrement nextIndex and retry
                            if (nextIndex.get(peer) > 1) {
                                nextIndex.put(peer, nextIndex.get(peer) - 1);
                                sendAppendEntries(peer, storage.getState(), new ArrayList<>());
                            }
                        }
                    }
                } finally {
                    sendAsyncLock.unlock();
                }
            } catch (Exception ignored) {}
        });
    }

    private void updateCommitIndex(RaftState state) {
        for (int n = commitIndex + 1; n <= state.log.size(); n++) {
            if (state.log.get(n - 1).term() == state.currentTerm) {
                int count = 1;
                for (int m : matchIndex.values()) {
                    if (m >= n) count++;
                }
                if (count > props.peers().size() / 2) {
                    commitIndex = n;
                    applyLog(state);
                }
            }
        }
    }

    private void applyLog(RaftState state) {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = state.log.get(lastApplied - 1);
            stateMachine.apply(entry.key(), entry.value());
        }
    }

    private void becomeFollower(int term, String leaderId) {
        lock.lock();
        try {
            RaftState state = storage.getState();
            state.currentTerm = term;
            state.votedFor = null;
            storage.save(state);

            role = Role.FOLLOWER;
            this.leaderId = leaderId;

            // Останавливаем heartbeat если был лидером
            if (heartbeatTimer != null) {
                heartbeatTimer.cancel(true);
            }

            // Сбрасываем election timer
//            electionTimerRunning = false;
//            resetElectionTimer();

            log.info("Became follower (term={}, node={}, leader={})", term, props.nodeId(), leaderId);
        } finally {
            lock.unlock();
        }
    }

    // --- Public API for Controller ---

    public StatusDto getStatus() {
        log.info("In status try get lock");
        lock.lock();
        log.info("In status lock acquired");
        try {
            RaftState state = storage.getState();
            return new StatusDto(role.name().toLowerCase(), state.currentTerm, leaderId, commitIndex, lastApplied, state.log.size());
        } finally {
            lock.unlock();
            log.info("In status lock freed");
        }
    }

    public String getValue(String key) {
        return stateMachine.get(key);
    }

    public CommandResp submitCommand(String key, String value) {
        log.info("in submitCommand try get lock");
        lock.lock();
        log.info("in submitCommand lock acquired");
        try {
            if (role != Role.LEADER) {
                return new CommandResp(null, getLeaderUrl());
            }

            RaftState state = storage.getState();
            int newIndex = state.log.size() + 1;
            LogEntry entry = new LogEntry(newIndex, state.currentTerm, key, value);
            state.log.add(entry);
            storage.save(state);

            // Отправляем heartbeat с новыми записями (они уже в логе)
            sendAppendEntriesToAll(state);

            // Ждём коммита
            long start = System.currentTimeMillis();
            while (commitIndex < newIndex && (System.currentTimeMillis() - start) < 2000) {
                try { Thread.sleep(10); } catch (InterruptedException e) {}
            }

            if (commitIndex >= newIndex) {
                return new CommandResp(newIndex, null);
            } else {
                return new CommandResp(null, null);
            }
        } finally {
            lock.unlock();
            log.info("In submitCommand log freed");
        }
    }

    private String getLeaderUrl() {
        if (leaderId == null) return null;
        // Simple mapping assumption: node1 -> http://node1:8080
        // In real env, need service discovery. Here we construct based on ID
        return "http://" + leaderId + ":8080";
    }

    private void sendAppendEntriesToAll(RaftState state) {
        for (String peer : props.peers()) {
            sendAppendEntries(peer, state, new ArrayList<>()); // Пустой список!
        }
    }

    // --- Internal RPC Handlers ---

    public VoteResp handleRequestVote(RequestVoteReq req) {
        lock.lock();
        try {
            RaftState state = storage.getState();
            if (req.term() > state.currentTerm) {
                becomeFollower(req.term(), req.candidateId());
            }

            if (req.term() < state.currentTerm) {
                return new VoteResp(state.currentTerm, false);
            }

            boolean logOk = (req.lastLogTerm() > getLastLogTerm(state)) ||
                    (req.lastLogTerm() == getLastLogTerm(state) && req.lastLogIndex() >= state.log.size());

            if ((state.votedFor == null || state.votedFor.equals(req.candidateId())) && logOk) {
                state.votedFor = req.candidateId();
                storage.save(state);
//                resetElectionTimer();
                return new VoteResp(state.currentTerm, true);
            }
            return new VoteResp(state.currentTerm, false);
        } finally {
            lock.unlock();
        }
    }

    private int getLastLogTerm(RaftState state) {
        return state.log.isEmpty() ? 0 : state.log.getLast().term();
    }

    public AppendResp handleAppendEntries(AppendEntriesReq req) {
        lock.lock();
        try {
            // 0. reset election timer
            resetElectionTimer();
            RaftState state = storage.getState();

            // 1. Если term больше — сразу становимся follower
            if (req.term() > state.currentTerm) {
                becomeFollower(req.term(), req.leaderId());
                state = storage.getState(); // Обновляем состояние
            }

            // 2. Если term меньше — отвергаем
            if (req.term() < state.currentTerm) {
                return new AppendResp(state.currentTerm, false);
            }

            // 4. Если были кандидатом — становимся follower
            if (role != Role.FOLLOWER) {
                becomeFollower(req.term(), req.leaderId());
            } else {
                leaderId = req.leaderId();
            }

            // 5. Проверка prevLogIndex
            if (req.prevLogIndex() > 0) {
                if (req.prevLogIndex() > state.log.size()) {
                    return new AppendResp(state.currentTerm, false);
                }
                int termAtPrev = state.log.get(req.prevLogIndex() - 1).term();
                if (termAtPrev != req.prevLogTerm()) {
                    return new AppendResp(state.currentTerm, false);
                }
            }

            // 6. Удаляем конфликтующие записи
            if (req.prevLogIndex() < state.log.size()) {
                for (int i = 0; i < req.entries().size(); i++) {
                    int index = req.prevLogIndex() + 1 + i;
                    if (index <= state.log.size()) {
                        if (state.log.get(index - 1).term() != req.entries().get(i).term()) {
                            state.log = new ArrayList<>(state.log.subList(0, index - 1));
                            break;
                        }
                    }
                }
            }

            // 7. Добавляем новые записи (ПРОВЕРЯЕМ ЧТОБЫ НЕ БЫЛО ДУБЛИКАТОВ)
            for (LogEntry entry : req.entries()) {
                // Пропускаем если запись уже есть
                if (entry.index() <= state.log.size()) {
                    continue;
                }
                state.log.add(entry);
            }
            storage.save(state);

            // 8. Обновляем commitIndex
            if (req.leaderCommit() > commitIndex) {
                commitIndex = Math.min(req.leaderCommit(), state.log.size());
                applyLog(state);
            }

            return new AppendResp(state.currentTerm, true);
        } finally {
            lock.unlock();
        }
    }
}