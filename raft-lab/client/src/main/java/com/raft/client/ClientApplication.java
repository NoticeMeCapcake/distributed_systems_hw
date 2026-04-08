package com.raft.client;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.restclient.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
public class ClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class, args);
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        CloseableHttpClient httpClient = HttpClients.custom()
                .disableRedirectHandling()
                .build();

        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient);
        return new RestTemplate(factory);
//        return builder
//                .connectTimeout(Duration.ofSeconds(1))
//                .readTimeout(Duration.ofSeconds(1))
//                .additionalInterceptors((request, body, execution) -> {
//                    // Не следуем за redirect автоматически
//                    return execution.execute(request, body);
//                })
//                .build();
    }

    @Bean
    public CommandLineRunner run(RestTemplate restTemplate) {
        return args -> {
            String urlsEnv = System.getenv("NODE_URLS");
            if (urlsEnv == null) urlsEnv = "http://localhost:8081,http://localhost:8082,http://localhost:8083";
            List<String> nodes = Arrays.asList(urlsEnv.split(","));
            Random rand = new Random();
            AtomicInteger ok = new AtomicInteger(0);
            AtomicInteger inconsistent = new AtomicInteger(0);
            AtomicInteger errors = new AtomicInteger(0);

            System.out.println("Client started. Nodes: " + nodes);
            System.out.println("Client: Sleep for 2000 ms");
            Thread.sleep(2000);

            while (true) {
                try {
                    // 1. Find Leader
                    String leaderUrl = null;
                    String randomNode = nodes.get(rand.nextInt(nodes.size()));

                    try {
                        ResponseEntity<Map> statusResp = restTemplate.getForEntity(randomNode + "/status", Map.class);
                        if (statusResp.getStatusCode().is2xxSuccessful()) {
                            String role = (String) statusResp.getBody().get("role");
                            if ("leader".equals(role)) {
                                leaderUrl = randomNode;
                                System.out.println("\nLeader found via /status: " + leaderUrl);
                            }
                        }
                    } catch (Exception e) {
                        // Node unreachable, continue to fallback
                    }

                    // Fallback: Try to find via command redirect
                    if (leaderUrl == null) {
                        for (String n : nodes) {
                            try {
                                HttpHeaders headers = new HttpHeaders();
                                headers.setContentType(MediaType.APPLICATION_JSON);
                                System.out.println("Sending command probe on " + n);
                                HttpEntity<Map<String, String>> entity = new HttpEntity<>(
                                        Map.of("key", "probe", "value", "0"),
                                        headers
                                );

                                ResponseEntity<Map> resp = restTemplate.exchange(
                                        n + "/command",
                                        HttpMethod.POST,
                                        entity,
                                        Map.class
                                );

                                System.out.println("Got response from probe: " + resp.getStatusCode().value());

                                if (resp.getStatusCode().value() == 302) {
                                    String location = resp.getHeaders().getFirst("Location");
                                    if (location != null) {
                                        leaderUrl = location.replace("/command", "");
                                        System.out.println("\nLeader found via 302: " + leaderUrl);
                                        break;
                                    }
                                }  else if (resp.getStatusCode().is2xxSuccessful()) {
                                    leaderUrl = n;
                                    System.out.println("\nLeader found via 200: " + leaderUrl);
                                    break;
                                }
                            } catch (Exception e) {
                                System.out.println("Got exception in probe: " + e);
                            }
                        }
                    }

                    if (leaderUrl == null) {
                        System.out.println("\nLeader not, retrying");
                        errors.incrementAndGet();
                        printStats(ok, inconsistent, errors);
                        Thread.sleep(500);
                        continue;
                    }

                    // 2. Send Command
                    String key = "k_" + System.currentTimeMillis();
                    String value = "v_" + rand.nextInt(1000);

                    HttpHeaders cmdHeaders = new HttpHeaders();
                    cmdHeaders.setContentType(MediaType.APPLICATION_JSON);
                    HttpEntity<Map<String, String>> cmdEntity = new HttpEntity<>(
                            Map.of("key", key, "value", value),
                            cmdHeaders
                    );

                    ResponseEntity<Map> cmdResp = restTemplate.exchange(
                            leaderUrl + "/command",
                            HttpMethod.POST,
                            cmdEntity,
                            Map.class
                    );

                    if (!cmdResp.getStatusCode().is2xxSuccessful()) {
                        errors.incrementAndGet();
                        printStats(ok, inconsistent, errors);
                        Thread.sleep(500);
                        continue;
                    }

                    // 3. Read from Random Node
                    String readNode = nodes.get(rand.nextInt(nodes.size()));
                    try {
                        System.out.println("Try to get value, key = " + key);
                        ResponseEntity<Map> getResp = restTemplate.getForEntity(
                                readNode + "/value/" + key,
                                Map.class
                        );
                        if (getResp.getStatusCode().is2xxSuccessful()) {
                            String readVal = (String) getResp.getBody().get("value");
                            if (value.equals(readVal)) {
                                ok.incrementAndGet();
                            } else {
                                inconsistent.incrementAndGet();
                            }
                        } else {
                            errors.incrementAndGet();
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }

                } catch (Exception e) {
                    errors.incrementAndGet();
                }

                printStats(ok, inconsistent, errors);
                Thread.sleep(200);
            }
        };
    }

    private void printStats(AtomicInteger ok, AtomicInteger inconsistent, AtomicInteger errors) {
        System.out.print("\rok: " + ok.get() + " | inconsistent: " + inconsistent.get() + " | errors: " + errors.get() + "   ");
    }
}