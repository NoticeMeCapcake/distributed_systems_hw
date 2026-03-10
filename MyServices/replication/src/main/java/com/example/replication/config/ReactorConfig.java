package com.example.replication.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Hooks;


@Configuration
public class ReactorConfig {

    @PostConstruct
    public void init() {
        // Игнорируем ошибки, возникшие из-за отмены подписки (например, от take())
        Hooks.onErrorDropped(error -> {
            if (error instanceof IllegalStateException &&
                    error.getMessage() != null &&
                    error.getMessage().contains("released already due to cancellation")) {
                // Тихо игнорируем
                return;
            }
            // Остальные ошибки логируем
            System.err.println("Dropped error: " + error.getMessage());
        });
    }
}