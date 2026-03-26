package com.sparta.teamssc.domain.chat.service;

import com.sparta.teamssc.domain.chat.entity.Message;
import com.sparta.teamssc.domain.chat.entity.RoomType;
import com.sparta.teamssc.domain.chat.repository.MessageRepository;
import com.sparta.teamssc.domain.period.service.PeriodService;
import com.sparta.teamssc.domain.team.service.TeamService;
import com.sparta.teamssc.domain.user.user.entity.User;
import com.sparta.teamssc.domain.user.user.service.UserService;
import com.sparta.teamssc.rabbitmq.RabbitMQConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageServiceImpl implements MessageService {

    private final MessageRepository messageRepository;
    private final TeamService teamService;
    private final PeriodService periodService;
    private final UserService userService;
    private final RabbitTemplate rabbitTemplate;

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
