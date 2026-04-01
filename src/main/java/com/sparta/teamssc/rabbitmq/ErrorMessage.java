package com.sparta.teamssc.rabbitmq;

import com.sparta.teamssc.domain.chat.entity.RoomType;
import jakarta.persistence.*;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "error_messages")
@Getter
@NoArgsConstructor
public class ErrorMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String messageId;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String content;

    @Column(nullable = false)
    private String sender;

    @Column(nullable = false)
    private Long roomId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private RoomType roomType;

    @Column(nullable = false)
    private LocalDateTime failedAt;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String failureReason;

    @Builder
    public ErrorMessage(String messageId, String content, String sender,
                        Long roomId, RoomType roomType,
                        LocalDateTime failedAt, String failureReason) {
        this.messageId = messageId;
        this.content = content;
        this.sender = sender;
        this.roomId = roomId;
        this.roomType = roomType;
        this.failedAt = failedAt;
        this.failureReason = failureReason;
    }
}
