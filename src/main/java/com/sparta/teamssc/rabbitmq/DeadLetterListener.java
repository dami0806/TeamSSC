package com.sparta.teamssc.rabbitmq;

import com.sparta.teamssc.domain.chat.entity.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * DLQ 메시지 뿐만 아닌
 * 아래 장애 이벤트 발생 시 추가로 Slack 알림을 보낼 수 있음.
 *
 * RabbitMQ 서킷 브레이커가 OPEN 상태일 때
 * WebSocket 재연결이 일정 횟수 초과할 때
 * RabbitMQ 큐가 특정 임계값 이상으로 쌓일 때 (500개 이상)
 * java
 * 복사
 * 편집
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DeadLetterListener {

    private final SlackNotificationService slackNotificationService;
    private static final int QUEUE_THRESHOLD = 500;

    @Scheduled(fixedRate = 60000) // 1분마다 실행
    public void checkQueueDepth() {
        int messageCount = getQueueMessageCount("chat-queue");
        if (messageCount > QUEUE_THRESHOLD) {
            slackNotificationService.sendNotification("⚠️ RabbitMQ 큐 메시지가 " + messageCount + "개 쌓였습니다!");
        }
    }

    private int getQueueMessageCount(String queueName) {
        // RabbitMQ Management API 로 메시지 개수 조회
        return 0;
    }
}
