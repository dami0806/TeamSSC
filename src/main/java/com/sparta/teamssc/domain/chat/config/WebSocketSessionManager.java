package com.sparta.teamssc.domain.chat.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@Component
public class WebSocketSessionManager {

    private final Set<WebSocketSession> sessions = Collections.synchronizedSet(new HashSet<>());

    public void addSession(WebSocketSession session) {
        sessions.add(session);
        log.info("새 WebSocket 세션 추가됨: 현재 활성 세션 수 = {}", sessions.size());
    }

    public void removeSession(WebSocketSession session) {
        sessions.remove(session);
        log.info("WebSocket 세션 제거됨: 현재 활성 세션 수 = {}", sessions.size());
    }

    public boolean hasActiveSessions() {
        return !sessions.isEmpty();
    }
}
