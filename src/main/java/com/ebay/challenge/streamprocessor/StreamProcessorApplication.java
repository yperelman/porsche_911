package com.ebay.challenge.streamprocessor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

/**
 * Main application for the Real-time Session Attribution Stream Processor.
 *
 * This application performs windowed joins between page view and ad click streams
 * to attribute page views to ad campaigns in real-time.
 */
@Slf4j
@SpringBootApplication
public class StreamProcessorApplication {

    public static void main(String[] args) {
        log.info("Starting Stream Processor Application...");
        SpringApplication.run(StreamProcessorApplication.class, args);
        log.info("Stream Processor Application started successfully");
    }

    /** Injectable wall-clock so wall-clock-relative metrics (e.g. dashboard lag) are testable. */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
