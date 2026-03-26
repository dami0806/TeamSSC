package com.sparta.teamssc.rabbitmq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
@Slf4j
public class SlackNotificationService {

    @Value("${SLACK_WEBHOOK_URL:}")
    private String slackWebhookUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    public void sendNotification(String message) {
        try {
            String payload = String.format("{\"text\": \"%s\"}", message);
            restTemplate.postForEntity(slackWebhookUrl, payload, String.class);
        } catch (IllegalArgumentException e) {
            log.error("슬렉으로 에러 보내기", e);
        }
    }
}