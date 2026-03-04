package com.example.myserviceb.service;

import com.example.myserviceb.dto.MessageForA;
import com.example.myserviceb.dto.MessageFromA;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ProcessorService {
    private static final AttributeKey<String> MESSAGE_ID = AttributeKey.stringKey("message_id");
    private final ConcurrentHashMap<UUID, MessageForA> idempotencyMap = new ConcurrentHashMap<>();
    private final LongCounter receivedMessagesCounter;

    public ProcessorService(OpenTelemetry openTelemetry) {
        this.receivedMessagesCounter = openTelemetry.getMeter("delivery-receiver-meter")
                .counterBuilder("delivery_messages_received_total")
                .setDescription("Total received messages by label message_id")
                .build();
    }

    private MessageForA generateResponse(UUID messageId) {
        return new MessageForA(messageId, UUID.randomUUID());
    }

    public MessageForA simpleProcess(MessageFromA request) {
        var response = generateResponse(request.messageId());

        incrementMessagesReceivedMetric(request);

        return response;
    }

    public MessageForA processIdempotently(MessageFromA request) {
        var response = checkIfExistsAndGetOrGenerateResponse(request);

        incrementMessagesReceivedMetric(request);

        return response;
    }

    private MessageForA checkIfExistsAndGetOrGenerateResponse(MessageFromA request) {
        var idempotencyKey = request.messageId();
        if (idempotencyMap.containsKey(idempotencyKey)) {
            return idempotencyMap.get(idempotencyKey);
        } else {
            return idempotencyMap.put(idempotencyKey, generateResponse(idempotencyKey));
        }
    }

    private void incrementMessagesReceivedMetric(MessageFromA request) {
        receivedMessagesCounter.add(1, Attributes.of(MESSAGE_ID, request.messageId().toString()));
    }
}
