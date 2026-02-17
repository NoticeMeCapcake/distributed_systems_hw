package com.example.myserviceb.controller;

import com.example.myserviceb.dto.MessageForA;
import com.example.myserviceb.dto.MessageFromA;
import com.example.myserviceb.service.ProcessorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/message")
public class BController {
    @Autowired
    private ProcessorService processorService;

    @PostMapping("/success")
    public String success() {
        return "Success from service B";
    }


    @PostMapping("/simple")
    public MessageForA simpleProcess(@RequestBody MessageFromA messageFromA) {
        return processorService.simpleProcess(messageFromA);
    }


    @PostMapping("/idempotency")
    public MessageForA idempotencyProcess(@RequestBody MessageFromA messageFromA) {
        return processorService.processIdempotently(messageFromA);
    }
}