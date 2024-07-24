package com.sparta.teamssc.domain.image.entity;

import com.sparta.teamssc.common.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Getter
@NoArgsConstructor
public class Image extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, unique = true)
    private String fileLink;

    @Builder
    public Image(String name, String fileLink) {
        this.fileLink = fileLink;
    }

}
