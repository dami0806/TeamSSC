package com.sparta.teamssc.domain.chat.config;

import com.sparta.teamssc.rabbitmq.SlackNotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.broker.BrokerAvailabilityEvent;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class BrokerStatusService {

    private final SimpMessagingTemplate messagingTemplate;
    private final SlackNotificationService slackNotificationService;

    @EventListener
    public void onBrokerAvailabilityEvent(BrokerAvailabilityEvent event) {
        if (event.isBrokerAvailable()) {
            log.info("STOMP 브로커 연결 복구됨");
            messagingTemplate.convertAndSend("/topic/broker-status",
                    Map.of("status", "CONNECTED", "message", "서버 연결이 복구되었습니다."));
        } else {
            log.warn("STOMP 브로커 연결 끊김");
            slackNotificationService.sendNotification("STOMP Broker 연결 끊김 감지");
            messagingTemplate.convertAndSend("/topic/broker-status",
                    Map.of("status", "DISCONNECTED", "message", "서버 연결이 불안정합니다. 잠시 후 자동으로 재연결됩니다."));
        }
    }
}
