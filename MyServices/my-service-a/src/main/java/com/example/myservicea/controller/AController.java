package com.example.myservicea.controller;

import com.example.myservicea.service.MessengerService;
import com.example.myservicea.dto.MessageFromB;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/api/message")
public class AController {

    private static final Logger log = LogManager.getLogger(AController.class);
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${service.b.url:http://service-b:80}")
    private String serviceBUrl;
    @Autowired
    private MessengerService messengerService;

    @PostMapping("/getError")
    public String getError() {
        log.info("getError");
        throw new RuntimeException("This is an error endpoint.");
    }

    @PostMapping("/at-most-once")
    public MessageFromB atMostOnce() {
        log.info("at-most-once");
        return messengerService.send();
    }

    @PostMapping("/at-least-once")
    public MessageFromB atLeastOnce() {
        log.info("at-leat-once");
        return messengerService.sendWithRetries();
    }

    @PostMapping("/exactly-once")
    public MessageFromB exactlyOnce() {
        log.info("exactly-once");
        return messengerService.sendIdempotently();
    }

    @PostMapping("/call-b")
    public String callB() {
        log.info("Call b");
        String response = restTemplate.postForObject(serviceBUrl + "/success", null, String.class);
        log.info("B answered");
        return "Response from B: " + response;
    }
}