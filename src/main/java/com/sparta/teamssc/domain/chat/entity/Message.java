package com.sparta.teamssc.domain.chat.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.sparta.teamssc.common.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Entity
@Getter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Message extends BaseEntity implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // 메시지 고유 식별자: Consumer 재처리 시 중복 저장 방지용
    @Column(nullable = false, unique = true)
    private String messageId;

    @Column(nullable = false)
    private String content;

    @Column(nullable = false)
    private String sender;

    @Column(nullable = false)
    private Long roomId;

    @Column(nullable = false)
    private RoomType roomType;

    @Builder
    public Message(String content, String sender, Long roomId, RoomType roomType) {
        this.messageId = UUID.randomUUID().toString();
        this.content = content;
        this.sender = sender;
        this.roomId = roomId;
        this.roomType = roomType;
    }
}