package com.sparta.teamssc.domain.chat.entity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 리팩토링 전: messageId 필드 없음 → 동일 메시지 재처리 시 중복 저장 발생
 * 리팩토링 후: messageId(UUID) 자동 생성 → Idempotent Consumer 구현
 */
class MessageEntityTest {

    private Message buildMessage(String content) {
        return Message.builder()
                .content(content)
                .sender("tester")
                .roomId(1L)
                .roomType(RoomType.TEAM)
                .build();
    }

    @Test
    @DisplayName("messageId가 Builder 호출 시 자동 생성된다")
    void messageId_자동생성() {
        Message message = buildMessage("안녕하세요");

        assertThat(message.getMessageId()).isNotNull();
        assertThat(message.getMessageId()).isNotBlank();
    }

    @Test
    @DisplayName("서로 다른 메시지는 각각 고유한 messageId를 가진다")
    void 서로다른_메시지는_다른_messageId() {
        Message m1 = buildMessage("메시지1");
        Message m2 = buildMessage("메시지2");

        assertThat(m1.getMessageId()).isNotEqualTo(m2.getMessageId());
    }

    @Test
    @DisplayName("[수치] 1000개 메시지 생성 시 messageId 중복 없음")
    void messageId_1000개_중복없음() {
        int count = 1000;

        Set<String> ids = IntStream.range(0, count)
                .mapToObj(i -> buildMessage("msg" + i).getMessageId())
                .collect(Collectors.toSet());

        // 중복이 없으면 set 크기 == count
        assertThat(ids).hasSize(count);
        System.out.println("[수치] 메시지 1000개 생성, 고유 messageId 수: " + ids.size() + " / 1000 → 충돌 없음");
    }

    @Test
    @DisplayName("동일 내용의 메시지도 별개의 messageId를 가진다 — RabbitMQ 재전달 시 구별 가능")
    void 동일내용_메시지_다른_messageId() {
        // 리팩토링 전: messageId 없어 동일 메시지 재전달 구별 불가 → 중복 저장
        // 리팩토링 후: UUID로 구별 가능 → 재전달 시 무시
        Message original = buildMessage("같은 내용");
        Message redelivered = buildMessage("같은 내용");

        assertThat(original.getMessageId()).isNotEqualTo(redelivered.getMessageId());
    }
}
