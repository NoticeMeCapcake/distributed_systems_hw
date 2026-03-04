package com.example.myservicea.service;

import com.example.myservicea.dto.MessageForB;
import com.example.myservicea.dto.MessageFromB;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.UUID;

@Service
public class MessengerService {

    private static final Logger log = LogManager.getLogger(MessengerService.class);
    private final RestTemplate restTemplate;
    private final String SIMPLE_ENDPOINT = "/simple";
    private final String IDEMPOTENT_ENDPOINT = "/idempotency";
    private final String ENDPOINT_PREFIX = "/api/message";
    private static final AttributeKey<String> MESSAGE_ID = AttributeKey.stringKey("message_id");

    private final LongCounter sentMessagesCounter;

    @Value("${service.b.url:http://service-b:80}")
    private String serviceBUrl;

    public MessengerService(OpenTelemetry openTelemetry) {
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectionRequestTimeout(1000); // время ожидания из пула соединений
        factory.setReadTimeout(1000);

        restTemplate = new RestTemplate(factory);

        this.sentMessagesCounter = openTelemetry.getMeter("delivery-sender-meter")
                .counterBuilder("delivery_messages_sent_total")
                .setDescription("Total sent messages by label message_id")
                .build();
    }

    private MessageForB generateMessage() {
        return new MessageForB(UUID.randomUUID());
    }

    private MessageFromB sendMessage(MessageForB request, String endpoint) {
        return restTemplate.postForObject(serviceBUrl + ENDPOINT_PREFIX + endpoint, request, MessageFromB.class);
    }

    public MessageFromB send() {
        var request = generateMessage();
        incrementMessagesSentMetric(request);

        return sendMessage(request, SIMPLE_ENDPOINT);
    }

    public MessageFromB sendWithRetries() {
        return sendSafely(SIMPLE_ENDPOINT);
    }

    public MessageFromB sendSafely(String endpoint) {
        var request = generateMessage();
        incrementMessagesSentMetric(request);

        while (true) {
            try {
                return sendMessage(request, endpoint);
            } catch (Exception ex) {
                log.info("Resending in 300 ms");
                try {
                    Thread.sleep(300);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

    }

    public MessageFromB sendIdempotently() {
        return sendSafely(IDEMPOTENT_ENDPOINT);
    }

    private void incrementMessagesSentMetric(MessageForB request) {
        sentMessagesCounter.add(1, Attributes.of(MESSAGE_ID, request.messageId().toString()));
    }
}
