package com.sparta.teamssc.rabbitmq;

import com.sparta.teamssc.domain.chat.entity.Message;
import com.sparta.teamssc.domain.chat.repository.MessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
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
    public void handleMessage(Message message) {
        log.debug("받은 RabbitMQ 메시지: {}", message);

        // 메시지 필드 검증: content가 없으면 DLQ로 이동
        if (message.getContent() == null || message.getContent().isEmpty()) {
            log.error("content가 비어있는 메시지 DLQ로 이동: {}", message);
            throw new AmqpRejectAndDontRequeueException("메시지 content가 비어있습니다.");
        }

        // 중복 메시지 확인 (Consumer 재처리 시 동일 messageId 재도달 방지)
        if (messageRepository.existsByMessageId(message.getMessageId())) {
            log.warn("이미 처리된 메시지 무시 - messageId: {}", message.getMessageId());
            return;
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