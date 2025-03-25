package com.sparta.teamssc.domain.chat.service;

import com.sparta.teamssc.domain.chat.config.WebSocketSessionManager;
import com.sparta.teamssc.domain.chat.entity.Message;
import com.sparta.teamssc.domain.chat.entity.RoomType;
import com.sparta.teamssc.domain.chat.repository.MessageRepository;
import com.sparta.teamssc.domain.period.service.PeriodService;
import com.sparta.teamssc.domain.team.service.TeamService;
import com.sparta.teamssc.domain.user.user.entity.User;
import com.sparta.teamssc.domain.user.user.service.UserService;
import com.sparta.teamssc.rabbitmq.RabbitMQConfig;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.security.Principal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageServiceImpl implements MessageService {

    private final MessageRepository messageRepository;
    private final TeamService teamService;
    private final PeriodService periodService;
    private final UserService userService;

    private final RabbitTemplate rabbitTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final WebSocketSessionManager webSocketSessionManager;
    private int reconnectAttempts = 0;
    private static final int MAX_RECONNECT_ATTEMPTS = 5;
    private static final String CIRCUIT_BREAKER_NAME = "webSocketCircuitBreaker";
    private static final String RETRY_NAME = "webSocketRetry";

    @Scheduled(fixedDelay = 1000) // 주기적으로 WebSocket 상태 확인
    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "handleWebSocketFailure")
    @Retry(name = RETRY_NAME)
    public void checkWebSocketConnection() {
        if (!isWebSocketConnected()) {
            reconnect();
        }
    }

    // websocket 연결 확인
    private boolean isWebSocketConnected() {
        boolean sessionActive = webSocketSessionManager.hasActiveSessions();

        try {
            messagingTemplate.convertAndSend("/topic/ping", "ping");
            log.info("WebSocket 테스트 메시지 전송 성공");
        } catch (Exception e) {
            log.error("WebSocket 테스트 메시지 전송 실패: {}", e.getMessage());
            sessionActive = false;
        }

        return sessionActive;
    }


    private void reconnect() {
        if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            log.error("최대 WebSocket 재연결 횟수 초과. 사용자에게 새로고침 요청.");
            alertUserToRefresh();
            return;
        }

        reconnectAttempts++;
        log.info("WebSocket 재연결 시도... (시도 횟수: {})", reconnectAttempts);
        connectWebSocket();
    }

//        int delay = (int) Math.min(1000 * Math.pow(2, reconnectAttempts), 30000); // 지수 백오프 (최대 30초)
//        reconnectAttempts++;
//
//        log.info("WebSocket 재연결 시도... (시도 횟수: {})", reconnectAttempts);
//
//        try {
//            Thread.sleep(delay); // 일정 시간 대기 후 재연결 시도
//            connectWebSocket();
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            log.error("WebSocket 재연결 중 오류 발생", e);
//        }
//    }

    private void connectWebSocket() {
        // TODO: 실제 WebSocket 재연결 로직 구현 필요
        log.info("WebSocket 재연결 성공!");
        reconnectAttempts = 0; // 재연결 성공 시 카운트 초기화
    }

    private void alertUserToRefresh() {
        log.warn("WebSocket 재연결 실패: 사용자에게 새로고침 요청");
        // TODO: 클라이언트에 새로고침 메시지 전달하는 로직 추가
    }

    @Transactional
    public void sendTeamMessage(Long teamId, String content) {

        User user = getCurrentUser();

        if (!teamService.isUserInTeam(user.getId(), teamId)) {
            throw new IllegalArgumentException("사용자가 해당 팀에 속해 있지 않습니다.");
        }

        Message message = Message.builder()
                .content(content)
                .sender(user.getUsername())
                .roomId(teamId)
                .roomType(RoomType.TEAM)
                .build();


        // 메시지를 RabbitMQ로 발행
        rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.QUEUE_NAME, message);
        log.info("팀Message 보내기 RabbitMQ: {}", message);
        log.info("메시지를 보낸 사람 이름: {}", user.getUsername());
    }

    @Transactional
    public void sendPeriodMessage(Long periodId, String content) {

        User user = getCurrentUser();

        if (!periodService.isUserInPeriod(user.getId(), periodId)) {
            throw new IllegalArgumentException("사용자가 해당 기간에 속해 있지 않습니다.");
        }

        Message message = Message.builder()
                .content(content)
                .sender(user.getUsername())
                .roomId(periodId)
                .roomType(RoomType.PERIOD)
                .build();

        // 메시지를 RabbitMQ로 발행
        rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.QUEUE_NAME, message);


        log.info("기수Message 보내기 RabbitMQ: {}", message);

    }

    // 팀 메시지을 불러오기
    @Override
    @Transactional(readOnly = true)
    public List<Message> getMessagesForTeam(Long teamId) {

        User user = getCurrentUser();

        if (!teamService.isUserInTeam(user.getId(), teamId)) {
            throw new IllegalArgumentException("사용자가 해당 팀에 속해 있지 않습니다.");
        }

        return messageRepository.findByRoomIdAndRoomType(teamId, RoomType.TEAM);
    }

    // 기수 메시지 불러오기
    @Override
    @Transactional(readOnly = true)
    public List<Message> getMessagesForPeriod(Long periodId) {

        User user = getCurrentUser();

        if (!periodService.isUserInPeriod(user.getId(), periodId) && !isManager(user)) {
            throw new IllegalArgumentException("사용자가 해당 기수에 속해 있지 않거나 관리자 권한이 없습니다.");
        }
        return messageRepository.findByRoomIdAndRoomType(periodId, RoomType.PERIOD);
    }

    /**
     * 3일지난 메시지는 삭제
     */
    @Override
    @Transactional
    public void deleteOldMessages() {
        LocalDateTime threeDaysAgo = LocalDateTime.now().minusDays(3);
        messageRepository.deleteByCreateAtBefore(threeDaysAgo);
    }

    private User getCurrentUser() {
        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if (principal instanceof UserDetails) {
            String username = ((UserDetails) principal).getUsername();
            return userService.getUserByEmail(username);
        } else {
            throw new IllegalStateException("인증된 사용자가 없습니다.");
        }
    }

    private boolean isManager(User user) {
        return user.getRoles().stream()
                .anyMatch(role -> role.getRole().equals("MANAGER"));
    }
}
