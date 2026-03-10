package com.example.replication.service;

import com.example.replication.config.ReplicaProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ReplicationService {

    private final ReplicaProperties properties;
    private final WebClient webClient;

    public int getDelayMaxMs() {
        return properties.getDelayMaxMs();
    }

    // Логика координатора записи
    public Mono<Boolean> coordinateWrite(long newValue) {
        List<String> peers = properties.getAllAddressesList();
        int w = properties.getW();

        // 1. Узнать максимальную версию у кворума
        return Flux.fromIterable(peers)
                .flatMap(addr -> webClient.get().uri(addr + "/internal/value")
                        .retrieve()
                        .bodyToMono(DataStore.Record.class)
                        .timeout(Duration.ofMillis(800))
                        .onErrorResume(e -> Mono.just(new DataStore.Record(0, -1))), 10) // Параллелизм
                .take(w) // Ждём W ответов
                .collectList()
                .map(records -> {
                    // Фильтруем записи с версией -1 (ошибочные)
                    var validRecords = records.stream()
                            .filter(r -> r.version() >= 0)
                            .toList();

                    if (validRecords.size() < w) return false;

                    long maxVer = validRecords.stream()
                            .mapToLong(DataStore.Record::version)
                            .max()
                            .orElse(0);
                    return maxVer + 1;
                })
                .flatMap(newVer -> {
                    // 2. Рассылка записи
                    return Flux.fromIterable(peers)
                            .flatMap(addr -> webClient.post().uri(addr + "/internal/value")
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .bodyValue(new DataStore.Record(newValue, (Long) newVer))
                                    .retrieve()
                                    .toBodilessEntity()
                                    .timeout(Duration.ofMillis(800))
                                    .onErrorResume(e -> Mono.empty()), 10)
                            .take(w)
                            .collectList()
                            .map(responses -> responses.size() >= w);
                });
    }

    // Логика координатора чтения
    public Mono<DataStore.Record> coordinateRead() {
        List<String> peers = properties.getAllAddressesList();
        int r = properties.getR();

        return Flux.fromIterable(peers)
                .flatMap(addr -> webClient.get().uri(addr + "/internal/value")
                        .retrieve()
                        .bodyToMono(DataStore.Record.class)
                        .timeout(Duration.ofMillis(800))
                        .onErrorResume(e -> Mono.just(new DataStore.Record(0, -1))), 10)
                .take(r)
                .collectList()
                .flatMap(records -> {
                    var validRecords = records.stream()
                            .filter(rec -> rec.version() >= 0)
                            .toList();

                    if (validRecords.size() < r) return Mono.empty();

                    // Выбираем запись с максимальной версией
                    var best = validRecords.stream()
                            .max(Comparator.comparingLong(DataStore.Record::version))
                            .orElse(new DataStore.Record(0, 0));
                    return Mono.just(best);
                });
    }
}