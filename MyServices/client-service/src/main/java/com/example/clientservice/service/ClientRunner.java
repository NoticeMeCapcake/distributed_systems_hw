package com.example.clientservice.service;

import com.example.clientservice.config.ClientProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
public class ClientRunner implements CommandLineRunner {

    private final ClientProperties properties;
    private final WebClient webClient;
    private final Random random = new Random();

    private final AtomicLong okCount = new AtomicLong(0);
    private final AtomicLong inconsistentCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public ClientRunner(ClientProperties properties, WebClient.Builder builder) {
        this.properties = properties;
        this.webClient = builder.build();
    }

    @Override
    public void run(String... args) throws Exception {
        List<String> replicas = properties.getReplicaAddressesList();
        if (replicas.isEmpty()) {
            log.error("No replicas configured");
            return;
        }

        log.info("Client started. Monitoring consistency...");

        while (true) {
            boolean success = false;
            var failedWriteTargets = new ArrayList<String>();
            var failedReadTargets = new ArrayList<String>();

            while (!success && (failedWriteTargets.size() < replicas.size() || failedReadTargets.size() < replicas.size())) {
                String target = getReplicaExceptGiven(replicas, failedWriteTargets);
                String readTarget = getReplicaExceptGiven(replicas, failedReadTargets);
                boolean isRead = false;

                if (target == null) {
                    break;
                }

                try {
                    // 1. Write
                    long writeVal = System.currentTimeMillis();

                    var writeResp = webClient.post()
                            .uri(target + "/value")
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue("{\"value\": " + writeVal + "}")
                            .retrieve()
                            .toBodilessEntity()
                            .block();

                    if (writeResp == null) {
                        failedWriteTargets.add(target);
                        continue; // Попробовать другую реплику
                    }

                    if (writeResp.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE) {
                        // Кворум не набран — это настоящая ошибка
                        errorCount.incrementAndGet();
                        printStatus();
                        break; // Не retry, это системная ошибка
                    }

                    if (!writeResp.getStatusCode().is2xxSuccessful()) {
                        failedWriteTargets.add(target);
                        continue; // Попробовать другую реплику
                    }

                    // 2. Read
                    isRead = true;
                    var readResp = webClient.get()
                            .uri(readTarget + "/value")
                            .retrieve()
                            .bodyToMono(Record.class)
                            .block();

                    if (readResp == null) {
                        failedReadTargets.add(readTarget);
                        continue; // Попробовать другую реплику
                    }

                    // 3. Compare
                    if (readResp.value() == writeVal) {
                        okCount.incrementAndGet();
                    } else {
                        inconsistentCount.incrementAndGet();
                    }

                    success = true; // Операция завершена успешно

                } catch (WebClientResponseException e) {
                    if (e.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE) {
                        // Кворум не набран — настоящая ошибка
                        errorCount.incrementAndGet();
                        printStatus();
                        break;
                    }
                    // Другие HTTP ошибки — пробуем другую реплику
                    if (isRead) {
                        failedReadTargets.add(readTarget);
                    } else {
                        failedWriteTargets.add(target);
                    }
                    continue;
                } catch (Exception e) {
                    // Ошибка соединения (реплика мертва) — пробуем другую
                    if (isRead) {
                        failedReadTargets.add(readTarget);
                    } else {
                        failedWriteTargets.add(target);
                    }
                    continue;
                }
            }

            if (failedWriteTargets.size() >= replicas.size() || failedReadTargets.size() >= replicas.size()) {
                // Все реплики попробовали, ничего не вышло
                errorCount.incrementAndGet();
            }

            printStatus();
            Thread.sleep(properties.getDelayBetweenOpsMs());
        }

//        while (true) {
//            try {
//                // 1. Write
//                String writeTarget = replicas.get(random.nextInt(replicas.size()));
//                long writeVal = System.currentTimeMillis(); // Уникальное значение
//
//                var writeResp = webClient.post()
//                        .uri(writeTarget + "/value")
//                        .contentType(MediaType.APPLICATION_JSON)
//                        .bodyValue("{\"value\": " + writeVal + "}")
//                        .retrieve()
//                        .toBodilessEntity()
//                        .block();
//
//                if (writeResp == null || !writeResp.getStatusCode().is2xxSuccessful()) {
//                    errorCount.incrementAndGet();
//                    printStatus();
//                    Thread.sleep(properties.getDelayBetweenOpsMs());
//                    continue;
//                }
//
//                // 2. Read
//                String readTarget = replicas.get(random.nextInt(replicas.size()));
//
//                var readResp = webClient.get()
//                        .uri(readTarget + "/value")
//                        .retrieve()
//                        .bodyToMono(Record.class)
//                        .block();
//
//                if (readResp == null) {
//                    errorCount.incrementAndGet();
//                    printStatus();
//                    Thread.sleep(properties.getDelayBetweenOpsMs());
//                    continue;
//                }
//
//                // 3. Compare
//                if (readResp.value() == writeVal) {
//                    okCount.incrementAndGet();
//                } else {
//                    inconsistentCount.incrementAndGet();
//                }
//
//                if ((++counter) % 10 == 0) {
//                    printStatus();
//                }
//            } catch (Exception e) {
//                errorCount.incrementAndGet();
//            } finally {
//                printStatus();
//                Thread.sleep(properties.getDelayBetweenOpsMs());
//            }
//        }
    }

    private void printStatus() {
        // \r возвращает курсор в начало строки для обновления "на месте"
        log.info("\nok: {} | inconsistent: {} | errors: {}",
                okCount.get(), inconsistentCount.get(), errorCount.get());
    }

    private String getReplicaExceptGiven(List<String> replicas, List<String> exception) {
        if (exception.size() >= replicas.size()) {
            return null;
        }
        var curReplica = "";
        do {
            curReplica = replicas.get(random.nextInt(replicas.size()));
        } while (exception.contains(curReplica));
        return curReplica;
    }

    public record Record(long value, long version) {}
}