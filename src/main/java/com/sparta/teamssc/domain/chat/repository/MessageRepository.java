package com.sparta.teamssc.domain.chat.repository;

import com.sparta.teamssc.domain.chat.entity.Message;
import com.sparta.teamssc.domain.chat.entity.RoomType;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;

public interface MessageRepository extends JpaRepository<Message, Long> {
    List<Message> findByRoomIdAndRoomType(Long teamId, RoomType roomType);
    void deleteByCreateAtBefore(LocalDateTime dateTime);
    boolean existsByMessageId(String messageId);
}
