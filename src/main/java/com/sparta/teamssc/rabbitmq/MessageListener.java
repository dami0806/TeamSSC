package com.sparta.teamssc.rabbitmq;

import com.sparta.teamssc.domain.chat.entity.Message;
import com.sparta.teamssc.domain.chat.repository.MessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageListener {

    private final SimpMessagingTemplate messagingTemplate;
    private final MessageRepository messageRepository;

    @RabbitListener(queues = RabbitMQConfig.QUEUE_NAME)
    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "handleMessageFailure")
    public void handleMessage(Message message) {
        log.debug("받은 RabbitMQ 메시지: {}", message);

        // 메시지 필드 검증
        if (message.getContent() == null || message.getContent().isEmpty()) {
            log.error("널값인 메시지: {}", message);
            return; // 메시지 처리를 중단
        }

        // 받은 메시지를 데이터베이스에 저장
        messageRepository.save(message);

        // 받은 메시지를 WebSocket으로 전송
       // String destination = "/topic/chat." + message.getRoomType().name().toLowerCase() + "." + message.getRoomId();
        String destination = "/app/chat/" + message.getRoomType().name().toLowerCase() + "/" + message.getRoomId();
        log.debug("보낸 WebSocket 도착지: {}", destination);

        messagingTemplate.convertAndSend(destination, message);
    }
}