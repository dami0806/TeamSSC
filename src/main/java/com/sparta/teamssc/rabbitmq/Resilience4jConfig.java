package com.sparta.teamssc.rabbitmq;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.RetryConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class Resilience4jConfig {

    // 서킷 브레이커 설정
    @Bean
    public CircuitBreakerConfig rabbitMQCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(50)  // 50% 이상 실패하면 차단
                .waitDurationInOpenState(Duration.ofSeconds(10))  // 10초 후 반열림
                .slidingWindowSize(10)  // 최근 10개 요청 기준으로 실패율 계산
                .build();
    }

    // WebSocket 재연결용 서킷 브레이커
    @Bean
    public CircuitBreakerConfig webSocketCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(40)  // 40% 이상 실패 시 차단
                .waitDurationInOpenState(Duration.ofSeconds(15))  // 15초 후 반열림
                .slidingWindowSize(5)  // 최근 5개 요청 기준으로 실패율 계산
                .build();
    }

    // WebSocket 재연결용 Retry 설정
    @Bean
    public RetryConfig webSocketRetryConfig() {
        return RetryConfig.custom()
                .maxAttempts(5)  // 최대 5번 재시도
                .waitDuration(Duration.ofSeconds(2))  // 2초 대기 후 재시도
                .build();
    }
}