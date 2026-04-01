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

import java.time.LocalDateTime;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadLetterListener {

    private final SlackNotificationService slackNotificationService;
    private final MessageRepository messageRepository;
    private final SimpMessagingTemplate messagingTemplate;
    private final RabbitTemplate rabbitTemplate;
    private final ErrorMessageRepository errorMessageRepository;

    private static final int QUEUE_THRESHOLD = 500;
    private static final int MAX_RETRY = 3;

    @Value("${spring.rabbitmq.host:localhost}")
    private String rabbitHost;

    @Value("${spring.rabbitmq.username:guest}")
    private String rabbitUsername;

    @Value("${spring.rabbitmq.password:guest}")
    private String rabbitPassword;

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * DLQ 메시지 수신 후 최대 3회 재처리 시도.
     * 모두 실패 시 error_messages 테이블에 영구 보존 후 ACK 전송
     * (NACK → 무한 루프 / ACK → 영구 유실 문제를 테이블 저장으로 해결)
     */
    @RabbitListener(queues = RabbitMQConfig.DEAD_LETTER_QUEUE)
    public void handleDeadLetter(Message message) {
        log.warn("DLQ 메시지 수신: {}", message);

        if (messageRepository.existsByMessageId(message.getMessageId())) {
            log.warn("DLQ: 이미 처리된 메시지 무시 - messageId: {}", message.getMessageId());
            return;
        }

        Exception lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRY; attempt++) {
            try {
                messageRepository.save(message);

                String destination = "/topic/chat/" + message.getRoomType().name().toLowerCase() + "/" + message.getRoomId();
                messagingTemplate.convertAndSend(destination, message);

                log.info("DLQ 메시지 재처리 성공 ({}회 시도): messageId={}", attempt, message.getMessageId());
                return;
            } catch (Exception e) {
                lastException = e;
                log.warn("DLQ 재처리 실패 ({}/{}회) - messageId={}, cause={}",
                        attempt, MAX_RETRY, message.getMessageId(), e.getMessage());
            }
        }

        // 3회 모두 실패 → error_messages 저장 후 ACK (유실 없이 무한 루프도 방지)
        String failureReason = lastException != null ? lastException.getMessage() : "알 수 없는 오류";
        errorMessageRepository.save(ErrorMessage.builder()
                .messageId(message.getMessageId())
                .content(message.getContent())
                .sender(message.getSender())
                .roomId(message.getRoomId())
                .roomType(message.getRoomType())
                .failedAt(LocalDateTime.now())
                .failureReason(failureReason)
                .build());

        log.error("DLQ {}회 재시도 최종 실패 - error_messages 저장 완료: messageId={}", MAX_RETRY, message.getMessageId());
        slackNotificationService.sendNotification(
                "DLQ " + MAX_RETRY + "회 재처리 최종 실패 - messageId: " + message.getMessageId()
                + ", sender: " + message.getSender()
                + ", roomType: " + message.getRoomType()
                + ", roomId: " + message.getRoomId()
                + ", 사유: " + failureReason
        );
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
