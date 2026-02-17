package com.example.myserviceb.dto;

import java.util.UUID;

public record MessageForA(
        UUID messageId,
        UUID response
) {
}