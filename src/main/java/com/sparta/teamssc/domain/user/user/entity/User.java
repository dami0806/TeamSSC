package com.sparta.teamssc.domain.user.user.entity;

import com.sparta.teamssc.common.entity.BaseEntity;
import com.sparta.teamssc.domain.team.entity.Team;
import com.sparta.teamssc.domain.user.refreshToken.entity.RefreshToken;
import com.sparta.teamssc.domain.user.role.userRole.UserRole;
import jakarta.persistence.*;
import lombok.*;

import java.util.HashSet;
import java.util.Set;

@Entity
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class User extends BaseEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String email;

    @Column(nullable = false)
    private String password;

    @Column(nullable = false)
    private String username;

    @Column(name = "git_link",nullable = true)
    private String gitLink;

    @Column(name = "vlog_link",nullable = true)
    private String vlogLink;

    @Column(nullable = true)
    private String intro;

    @Column(nullable = true)
    private String mbti;

    @Column(nullable = true)
    private String hobby;

    @Enumerated(EnumType.STRING)
    @Column(nullable = true)
    private UserStatus status;

    @Column(name = "profile_image",nullable = true)
    private String profileImage;

    @Column(nullable = true)
    private String section;

    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<UserRole> roles = new HashSet<>();

    @OneToOne(mappedBy = "user", cascade = CascadeType.ALL, orphanRemoval = true)
    private RefreshToken refreshToken;

    @ManyToOne
    @JoinColumn(name = "team_id")
    private Team team;

    @Builder
    public User(String email, String password, String username, UserStatus status) {
        this.email = email;
        this.password = password;
        this.username = username;
        this.status = status;
    }

    public void login() {
        this.status = UserStatus.ACTIVE;
    }


}
