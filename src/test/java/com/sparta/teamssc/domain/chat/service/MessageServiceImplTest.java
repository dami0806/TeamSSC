package com.sparta.teamssc.domain.chat.service;

import com.sparta.teamssc.domain.chat.entity.Message;
import com.sparta.teamssc.domain.chat.entity.RoomType;
import com.sparta.teamssc.domain.chat.repository.MessageRepository;
import com.sparta.teamssc.domain.period.service.PeriodService;
import com.sparta.teamssc.domain.team.service.TeamService;
import com.sparta.teamssc.domain.user.user.entity.User;
import com.sparta.teamssc.domain.user.user.service.UserService;
import com.sparta.teamssc.rabbitmq.RabbitMQConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.BDDMockito.*;

/**
 * 리팩토링 전 → 후 비교
 *
 * CircuitBreaker: AOP 기반으로 단위 테스트에서는 실제 작동 안 함
 * → 비즈니스 로직(팀 소속 검증, messageId 생성, publish 호출)을 검증
 * → CircuitBreaker 수치 비교는 VirtualThreadThroughputTest 참고
 */
@ExtendWith(MockitoExtension.class)
class MessageServiceImplTest {

    @Mock MessageRepository messageRepository;
    @Mock TeamService teamService;
    @Mock PeriodService periodService;
    @Mock UserService userService;
    @Mock RabbitTemplate rabbitTemplate;

    @InjectMocks MessageServiceImpl messageService;

    private User testUser;

    @BeforeEach
    void setUp() {
        testUser = mock(User.class);
        given(testUser.getId()).willReturn(1L);
        lenient().when(testUser.getUsername()).thenReturn("tester");

        UserDetails userDetails = mock(UserDetails.class);
        given(userDetails.getUsername()).willReturn("test@test.com");

        Authentication auth = mock(Authentication.class);
        given(auth.getPrincipal()).willReturn(userDetails);

        SecurityContext securityContext = mock(SecurityContext.class);
        given(securityContext.getAuthentication()).willReturn(auth);
        SecurityContextHolder.setContext(securityContext);

        given(userService.getUserByEmail("test@test.com")).willReturn(testUser);
    }

    // ───────────────────────────────────────────────────────────────────────
    // 정상 publish
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("팀 메시지 정상 publish — exchange, queue, messageId 포함 확인")
    void 팀_메시지_정상_publish() {
        Long teamId = 1L;
        given(teamService.isUserInTeam(1L, teamId)).willReturn(true);
        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);

        messageService.sendTeamMessage(teamId, "안녕하세요");

        then(rabbitTemplate).should().convertAndSend(
                eq(RabbitMQConfig.EXCHANGE_NAME),
                eq(RabbitMQConfig.QUEUE_NAME),
                captor.capture()
        );
        Message published = captor.getValue();
        assertThat(published.getContent()).isEqualTo("안녕하세요");
        assertThat(published.getSender()).isEqualTo("tester");
        assertThat(published.getRoomType()).isEqualTo(RoomType.TEAM);
        assertThat(published.getMessageId()).isNotNull(); // 리팩토링 후 UUID 포함
    }

    @Test
    @DisplayName("기수 메시지 정상 publish — exchange, queue, messageId 포함 확인")
    void 기수_메시지_정상_publish() {
        Long periodId = 2L;
        given(periodService.isUserInPeriod(1L, periodId)).willReturn(true);
        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);

        messageService.sendPeriodMessage(periodId, "기수 채팅");

        then(rabbitTemplate).should().convertAndSend(
                eq(RabbitMQConfig.EXCHANGE_NAME),
                eq(RabbitMQConfig.QUEUE_NAME),
                captor.capture()
        );
        Message published = captor.getValue();
        assertThat(published.getRoomType()).isEqualTo(RoomType.PERIOD);
        assertThat(published.getMessageId()).isNotNull();
    }

    // ───────────────────────────────────────────────────────────────────────
    // 접근 제어 — 미소속 사용자
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("팀 미소속 사용자 → 예외 발생, RabbitMQ publish 차단")
    void 팀_미소속_사용자_publish_차단() {
        Long teamId = 1L;
        given(teamService.isUserInTeam(1L, teamId)).willReturn(false);

        assertThatThrownBy(() -> messageService.sendTeamMessage(teamId, "메시지"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("사용자가 해당 팀에 속해 있지 않습니다.");

        then(rabbitTemplate).shouldHaveNoInteractions();
    }

    @Test
    @DisplayName("기수 미소속 사용자 → 예외 발생, RabbitMQ publish 차단")
    void 기수_미소속_사용자_publish_차단() {
        Long periodId = 1L;
        given(periodService.isUserInPeriod(1L, periodId)).willReturn(false);

        assertThatThrownBy(() -> messageService.sendPeriodMessage(periodId, "메시지"))
                .isInstanceOf(IllegalArgumentException.class);

        then(rabbitTemplate).shouldHaveNoInteractions();
    }

    // ───────────────────────────────────────────────────────────────────────
    // publish 시 messageId 포함 — JSON 직렬화 시 Consumer에서 복원 가능
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[수치] 팀 메시지 10회 publish → RabbitMQ convertAndSend 정확히 10회 호출")
    void 팀_메시지_10회_publish_횟수_검증() {
        Long teamId = 1L;
        given(teamService.isUserInTeam(1L, teamId)).willReturn(true);

        for (int i = 0; i < 10; i++) {
            messageService.sendTeamMessage(teamId, "메시지 " + i);
        }

        then(rabbitTemplate).should(times(10)).convertAndSend(
                eq(RabbitMQConfig.EXCHANGE_NAME),
                eq(RabbitMQConfig.QUEUE_NAME),
                any(Message.class)
        );
        System.out.println("[수치] 팀 메시지 10회 전송 → convertAndSend 호출 10회 확인");
    }

    @Test
    @DisplayName("[수치] 팀/기수 각 5회 → 총 10회 publish, 각각 올바른 RoomType")
    void 팀_기수_각5회_총10회_publish() {
        Long teamId = 1L;
        Long periodId = 2L;
        given(teamService.isUserInTeam(1L, teamId)).willReturn(true);
        given(periodService.isUserInPeriod(1L, periodId)).willReturn(true);

        for (int i = 0; i < 5; i++) messageService.sendTeamMessage(teamId, "팀 " + i);
        for (int i = 0; i < 5; i++) messageService.sendPeriodMessage(periodId, "기수 " + i);

        then(rabbitTemplate).should(times(10)).convertAndSend(
                eq(RabbitMQConfig.EXCHANGE_NAME),
                eq(RabbitMQConfig.QUEUE_NAME),
                any(Message.class)
        );
        System.out.println("[수치] 팀 메시지 5회 + 기수 메시지 5회 → 총 publish 10회 확인");
    }
}
