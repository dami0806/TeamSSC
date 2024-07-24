package com.sparta.teamssc.domain.board.board.dto;

import lombok.Getter;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.List;

@Getter
public class BoardRequestDto {
    private String title;
    private String content;
    private List<MultipartFile> images;

    public BoardRequestDto(String title, String content, List<MultipartFile> images) {
        this.title = title;
        this.content = content;
        this.images = images;
    }

}
