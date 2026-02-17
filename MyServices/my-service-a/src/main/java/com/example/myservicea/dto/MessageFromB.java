package com.example.myservicea.dto;

import java.util.UUID;

public record MessageFromB(
        UUID messageId,
        UUID response
) {
}