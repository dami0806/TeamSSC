package com.sparta.teamssc.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class MetricsConfig {

    private final RabbitAdmin rabbitAdmin;

    public MetricsConfig(RabbitAdmin rabbitAdmin) {
        this.rabbitAdmin = rabbitAdmin;
    }

    @Bean
    public void monitorQueueSize(MeterRegistry meterRegistry) {
        Gauge.builder("rabbitmq.queue.size", () -> {
            int queueSize = rabbitAdmin.getQueueProperties("chatQueue").size();
            log.info("📊 RabbitMQ chatQueue 현재 크기: {}", queueSize);
            return queueSize;
        }).register(meterRegistry);
    }
}
