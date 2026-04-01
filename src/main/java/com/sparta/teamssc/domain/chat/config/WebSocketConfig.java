package com.sparta.teamssc.domain.chat.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
@RequiredArgsConstructor
@Slf4j
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {


    private final WebSocketAuthInterceptor webSocketAuthInterceptor; // 인증정보 보안 컨텍스트에 세팅
    private final WebSocketSecurityContextChannelInterceptor securityContextChannelInterceptor; // 보안 컨텍스트에 있는걸 쓰레드 컨텍트스 홀더에 셍팅 및 지춤

    @Value("${spring.rabbitmq.host:localhost}")
    private String rabbitHost;


    @Bean
    public ThreadPoolTaskScheduler stompRelayTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadFactory(Thread.ofVirtual().name("stomp-relay-", 0).factory());
        scheduler.initialize();
        return scheduler;
    }

    // 메시지 브로커 app인걸 라우팅
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        log.debug("STOMP Broker Relay 설정 중");
        registry.setPathMatcher(new AntPathMatcher("/"));
        registry.enableStompBrokerRelay("/topic", "/queue", "/exchange", "/amq/queue", "/chat")
                .setRelayHost(rabbitHost)
                .setRelayPort(61613)
                .setClientLogin("guest")
                .setClientPasscode("guest")
                .setVirtualHost("/")
                .setSystemHeartbeatSendInterval(10000)
                .setSystemHeartbeatReceiveInterval(10000)
                .setTaskScheduler(stompRelayTaskScheduler());
        registry.setApplicationDestinationPrefixes("/app");
    }

    // STOMP 엔드포인트를 등록
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
          registry.addEndpoint("/wss/init")
                .setAllowedOriginPatterns("*");        // .withSockJS();
    }

    // 클라이언트 인바운드 채널에 인터셉터 (인증정보랑 컨텍스트 홀더 세팅 관련 인터셉트)
    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(webSocketAuthInterceptor, securityContextChannelInterceptor);

        // WebSocket 연결 종료 감지 핸들러
        registration.interceptors(new ChannelInterceptor() {
            @Override
            public void postSend(Message<?> message, MessageChannel channel, boolean sent) {
                StompHeaderAccessor accessor = StompHeaderAccessor.wrap(message);
                if (StompCommand.DISCONNECT.equals(accessor.getCommand())) {
                    String sessionId = accessor.getSessionId();

                    // 원인을 정확히 알 수 없으므로, 단순 로그 남기고 클라이언트에 재연결 요청 가능
                    log.warn("WebSocket 연결 종료 감지 - 세션 ID: {}", sessionId);

                }
            }
        });
    }
}
