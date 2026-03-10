package com.example.replication.controller;

import com.example.replication.service.DataStore;
import com.example.replication.service.ReplicationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequiredArgsConstructor
public class ReplicaController {

    private final ReplicationService replicationService;
    private final DataStore dataStore;

    // Внешний API: Запись
    @PostMapping("/value")
    public Mono<ResponseEntity<Map<String, String>>> write(@RequestBody Map<String, Long> body) {
        long val = body.getOrDefault("value", 0L);
        return replicationService.coordinateWrite(val)
                .map(success -> success ? ResponseEntity.ok(Map.of("status", "ok"))
                        : ResponseEntity.status(503).body(Map.of("status", "unavailable")));
    }

    // Внешний API: Чтение
    @GetMapping("/value")
    public Mono<ResponseEntity<DataStore.Record>> read() {
        return replicationService.coordinateRead()
                .map(record -> ResponseEntity.ok(record))
                .defaultIfEmpty(ResponseEntity.status(503).body(new DataStore.Record(0, 0)));
    }

    // Внутренний API: Получить локальное
    @GetMapping("/internal/value")
    public DataStore.Record getInternal() {
        return dataStore.get();
    }

    // Внутренний API: Обновить локальное (с задержкой)
    @PostMapping("/internal/value")
    public Mono<ResponseEntity<Map<String, String>>> updateInternal(@RequestBody DataStore.Record record) {
        return Mono.fromCallable(() -> {
            // Имитация задержки репликации
            int delay = (int) (Math.random() * replicationService.getDelayMaxMs());
            Thread.sleep(delay);

            boolean updated = dataStore.updateIfNewer(record.value(), record.version());
            return ResponseEntity.ok(Map.of("updated", String.valueOf(updated)));
        }).onErrorResume(e -> Mono.just(ResponseEntity.status(500).build()));
    }
}