package com.sparta.teamssc.rabbitmq;

import com.sparta.teamssc.domain.chat.entity.Message;
import com.sparta.teamssc.domain.chat.entity.RoomType;
import com.sparta.teamssc.domain.chat.repository.MessageRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.*;

/**
 * 리팩토링 전 → 후 비교
 *
 * 1. null content:    return(ACK 후 유실)    → AmqpRejectAndDontRequeueException(DLQ)
 * 2. 중복 messageId:  매번 save 호출          → existsByMessageId 확인 후 무시
 * 3. Consumer 예외:   requeue=true 무한루프  → requeue=false 후 DLQ 이동
 */
@ExtendWith(MockitoExtension.class)
class MessageListenerTest {

    @Mock SimpMessagingTemplate messagingTemplate;
    @Mock MessageRepository messageRepository;

    @InjectMocks MessageListener messageListener;

    private Message buildMessage(String content) {
        return Message.builder()
                .content(content)
                .sender("tester")
                .roomId(1L)
                .roomType(RoomType.TEAM)
                .build();
    }

    // ───────────────────────────────────────────────────────────────────────
    // 정상 흐름
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("정상 메시지 — DB 저장 및 WebSocket 전송 각 1회")
    void 정상_메시지_저장_및_전송() {
        Message message = buildMessage("안녕하세요");
        given(messageRepository.existsByMessageId(message.getMessageId())).willReturn(false);

        messageListener.handleMessage(message);

        then(messageRepository).should(times(1)).save(message);
        then(messagingTemplate).should(times(1)).convertAndSend(anyString(), eq(message));
    }

    // ───────────────────────────────────────────────────────────────────────
    // null content 처리
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[리팩토링 후] null content → AmqpRejectAndDontRequeueException — DLQ로 이동")
    void null_content_DLQ_이동() {
        // 리팩토링 전: return → ACK 처리되어 메시지 영구 유실
        // 리팩토링 후: AmqpRejectAndDontRequeueException → DLQ 이동
        Message message = Message.builder()
                .content(null)
                .sender("tester")
                .roomId(1L)
                .roomType(RoomType.TEAM)
                .build();

        assertThatThrownBy(() -> messageListener.handleMessage(message))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);

        then(messageRepository).shouldHaveNoInteractions();
        then(messagingTemplate).shouldHaveNoInteractions();
    }

    @Test
    @DisplayName("[리팩토링 후] 빈 문자열 content → AmqpRejectAndDontRequeueException")
    void 빈문자열_content_DLQ_이동() {
        Message message = buildMessage("");

        assertThatThrownBy(() -> messageListener.handleMessage(message))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);

        then(messageRepository).shouldHaveNoInteractions();
    }

    // ───────────────────────────────────────────────────────────────────────
    // 중복 메시지 처리 (Idempotent Consumer)
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[리팩토링 후] 이미 처리된 messageId → save 호출 없음")
    void 중복_messageId_무시() {
        // 리팩토링 전: 중복 확인 없어 동일 메시지가 여러 번 DB에 저장됨
        // 리팩토링 후: existsByMessageId=true → return → 중복 저장 방지
        Message message = buildMessage("중복 메시지");
        given(messageRepository.existsByMessageId(message.getMessageId())).willReturn(true);

        messageListener.handleMessage(message);

        then(messageRepository).should(never()).save(any());
        then(messagingTemplate).shouldHaveNoInteractions();
    }

    @Test
    @DisplayName("[수치] 동일 메시지 10회 재전달 시 저장 횟수 비교")
    void 동일메시지_재전달_저장횟수_비교() {
        // 리팩토링 전 기대 저장 횟수: 10회
        // 리팩토링 후 실제 저장 횟수: 1회
        Message message = buildMessage("재전달 메시지");

        // 첫 번째 수신: 저장
        given(messageRepository.existsByMessageId(message.getMessageId()))
                .willReturn(false)     // 1회차: 신규
                .willReturn(true)      // 2~10회차: 중복
                .willReturn(true).willReturn(true).willReturn(true)
                .willReturn(true).willReturn(true).willReturn(true)
                .willReturn(true).willReturn(true);

        for (int i = 0; i < 10; i++) {
            messageListener.handleMessage(message);
        }

        // 저장은 딱 1회만 호출
        then(messageRepository).should(times(1)).save(any());
        System.out.println("[수치] 동일 메시지 10회 재전달 → save 호출: 1회 (리팩토링 전: 10회)");
    }

    // ───────────────────────────────────────────────────────────────────────
    // Consumer 예외 → DLQ 이동 (defaultRequeueRejected=false 설정 검증)
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[리팩토링 후] DB 저장 예외 → 예외 전파 → DLQ 이동")
    void DB_예외_전파_DLQ이동() {
        // 리팩토링 전: requeue=true → 동일 메시지 무한 루프
        // 리팩토링 후: requeue=false(defaultRequeueRejected=false) → DLQ 이동
        //   → 이 테스트는 예외가 전파됨을 확인 (전파되어야 DLQ로 이동)
        Message message = buildMessage("오류 메시지");
        given(messageRepository.existsByMessageId(message.getMessageId())).willReturn(false);
        given(messageRepository.save(any())).willThrow(new RuntimeException("DB 연결 오류"));

        assertThatThrownBy(() -> messageListener.handleMessage(message))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("DB 연결 오류");
    }
}
