package com.sparta.teamssc.rabbitmq;

import com.sparta.teamssc.domain.chat.entity.Message;
import com.sparta.teamssc.domain.chat.entity.RoomType;
import com.sparta.teamssc.domain.chat.repository.MessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.BDDMockito.*;

/**
 * 리팩토링 전: DLQ Consumer 없음 → DLQ에 쌓여도 아무 처리 없음
 * 리팩토링 후: DLQ Consumer 구현 → 재처리 시도 → 실패 시 Slack 알림
 */
@ExtendWith(MockitoExtension.class)
class DeadLetterListenerTest {

    @Mock SlackNotificationService slackNotificationService;
    @Mock MessageRepository messageRepository;
    @Mock SimpMessagingTemplate messagingTemplate;
    @Mock RabbitTemplate rabbitTemplate;

    @InjectMocks DeadLetterListener deadLetterListener;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(deadLetterListener, "rabbitHost", "localhost");
        ReflectionTestUtils.setField(deadLetterListener, "rabbitUsername", "guest");
        ReflectionTestUtils.setField(deadLetterListener, "rabbitPassword", "guest");
    }

    private Message buildMessage() {
        return Message.builder()
                .content("DLQ 재처리 메시지")
                .sender("tester")
                .roomId(1L)
                .roomType(RoomType.TEAM)
                .build();
    }

    // ───────────────────────────────────────────────────────────────────────
    // DLQ 정상 재처리
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[리팩토링 후] DLQ 메시지 재처리 성공 — DB 저장 및 WebSocket 전송")
    void DLQ_재처리_성공() {
        // 리팩토링 전: DLQ Consumer 없어 재처리 불가
        // 리팩토링 후: 재처리 성공 → DB 저장 + WebSocket 전송
        Message message = buildMessage();
        given(messageRepository.existsByMessageId(message.getMessageId())).willReturn(false);

        deadLetterListener.handleDeadLetter(message);

        then(messageRepository).should(times(1)).save(message);
        then(messagingTemplate).should(times(1)).convertAndSend(anyString(), eq(message));
        then(slackNotificationService).shouldHaveNoInteractions();
    }

    // ───────────────────────────────────────────────────────────────────────
    // DLQ 중복 메시지
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[리팩토링 후] DLQ 중복 messageId → 재처리 없이 무시")
    void DLQ_중복_messageId_무시() {
        Message message = buildMessage();
        given(messageRepository.existsByMessageId(message.getMessageId())).willReturn(true);

        deadLetterListener.handleDeadLetter(message);

        then(messageRepository).should(never()).save(any());
        then(messagingTemplate).shouldHaveNoInteractions();
        then(slackNotificationService).shouldHaveNoInteractions();
    }

    // ───────────────────────────────────────────────────────────────────────
    // DLQ 재처리 실패 → Slack 알림
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[리팩토링 후] DLQ 재처리 실패 → Slack 알림 발송")
    void DLQ_재처리_실패_Slack_알림() {
        // 리팩토링 전: DLQ Consumer 없어 알림 불가
        // 리팩토링 후: 재처리 실패 → Slack으로 개발자에게 즉시 알림
        Message message = buildMessage();
        given(messageRepository.existsByMessageId(message.getMessageId())).willReturn(false);
        given(messageRepository.save(any())).willThrow(new RuntimeException("DB 오류"));

        deadLetterListener.handleDeadLetter(message);

        then(slackNotificationService).should(times(1)).sendNotification(anyString());
        System.out.println("[검증] DLQ 재처리 실패 시 Slack 알림 1회 발송 확인");
    }

    @Test
    @DisplayName("[수치] DLQ 재처리 실패 시 save 재시도 없이 즉시 Slack — 무한루프 없음")
    void DLQ_재처리_save_재시도_없음() {
        Message message = buildMessage();
        given(messageRepository.existsByMessageId(message.getMessageId())).willReturn(false);
        given(messageRepository.save(any())).willThrow(new RuntimeException("DB 오류"));

        deadLetterListener.handleDeadLetter(message);

        // save가 정확히 1회만 시도됨 — 무한 재시도 없음
        then(messageRepository).should(times(1)).save(any());
        then(slackNotificationService).should(times(1)).sendNotification(anyString());
        System.out.println("[수치] DLQ 재처리 실패 → save 시도: 1회, Slack 알림: 1회 (무한 재시도 없음)");
    }

    @Test
    @DisplayName("[수치] DLQ 메시지 5개 중 3개 성공, 2개 실패 시 Slack 2회 발송")
    void DLQ_복수_메시지_처리_통계() {
        int totalMessages = 5;
        int failCount = 2;

        for (int i = 0; i < totalMessages; i++) {
            Message message = buildMessage();
            given(messageRepository.existsByMessageId(message.getMessageId())).willReturn(false);
            if (i < failCount) {
                given(messageRepository.save(message)).willThrow(new RuntimeException("오류"));
            }
            deadLetterListener.handleDeadLetter(message);
        }

        then(slackNotificationService).should(times(failCount)).sendNotification(anyString());
        System.out.printf("[수치] DLQ 처리 %d건 중 성공 %d건, 실패 %d건 → Slack 알림 %d회%n",
                totalMessages, totalMessages - failCount, failCount, failCount);
    }
}
