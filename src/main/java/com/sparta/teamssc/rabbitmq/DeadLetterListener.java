package com.sparta.teamssc.rabbitmq;

import com.sparta.teamssc.domain.chat.entity.Message;
import com.sparta.teamssc.domain.chat.repository.MessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadLetterListener {

    private final SlackNotificationService slackNotificationService;
    private final MessageRepository messageRepository;
    private final SimpMessagingTemplate messagingTemplate;
    private final RabbitTemplate rabbitTemplate;

    private static final int QUEUE_THRESHOLD = 500;

    @Value("${spring.rabbitmq.host:localhost}")
    private String rabbitHost;

    @Value("${spring.rabbitmq.username:guest}")
    private String rabbitUsername;

    @Value("${spring.rabbitmq.password:guest}")
    private String rabbitPassword;

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * DLQ 메시지 수신 후 재처리 시도.
     * DB 저장 및 WebSocket 전송 재시도 후 실패 시 Slack 알림.
     */
    @RabbitListener(queues = RabbitMQConfig.DEAD_LETTER_QUEUE)
    public void handleDeadLetter(Message message) {
        log.warn("DLQ 메시지 수신: {}", message);
        try {
            if (messageRepository.existsByMessageId(message.getMessageId())) {
                log.warn("DLQ: 이미 처리된 메시지 무시 - messageId: {}", message.getMessageId());
                return;
            }

            messageRepository.save(message);

            String destination = "/topic/chat/" + message.getRoomType().name().toLowerCase() + "/" + message.getRoomId();
            messagingTemplate.convertAndSend(destination, message);

            log.info("DLQ 메시지 재처리 성공: {}", message);
        } catch (Exception e) {
            log.error("DLQ 메시지 재처리 실패, Slack 알림 전송: {}", message, e);
            slackNotificationService.sendNotification(
                    "DLQ 메시지 재처리 실패 - roomType: " + message.getRoomType()
                            + ", roomId: " + message.getRoomId()
                            + ", sender: " + message.getSender()
            );
        }
    }

    @Scheduled(fixedRate = 60000)
    public void checkQueueDepth() {
        int messageCount = getQueueMessageCount(RabbitMQConfig.QUEUE_NAME);
        if (messageCount > QUEUE_THRESHOLD) {
            slackNotificationService.sendNotification("RabbitMQ 큐 메시지가 " + messageCount + "개 쌓였습니다!");
        }
    }

    private int getQueueMessageCount(String queueName) {
        try {
            String url = String.format(
                    "http://%s:15672/api/queues/%%2F/%s",
                    rabbitHost, queueName
            );

            restTemplate.getInterceptors().clear();
            restTemplate.getInterceptors().add((request, body, execution) -> {
                String credentials = rabbitUsername + ":" + rabbitPassword;
                String encoded = java.util.Base64.getEncoder().encodeToString(credentials.getBytes());
                request.getHeaders().set("Authorization", "Basic " + encoded);
                return execution.execute(request, body);
            });

            ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
            if (response.getBody() != null && response.getBody().containsKey("messages")) {
                return ((Number) response.getBody().get("messages")).intValue();
            }
        } catch (Exception e) {
            log.warn("RabbitMQ Management API 조회 실패: {}", e.getMessage());
        }
        return 0;
    }
}
