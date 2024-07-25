package com.sparta.teamssc.domain.board.comment.service;

import com.sparta.teamssc.domain.board.board.entity.Board;
import com.sparta.teamssc.domain.board.board.service.BoardService;
import com.sparta.teamssc.domain.board.comment.dto.request.CommentRequestDto;
import com.sparta.teamssc.domain.board.comment.dto.response.CommentResponseDto;
import com.sparta.teamssc.domain.board.comment.entity.Comment;
import com.sparta.teamssc.domain.board.comment.repository.CommentRepository;
import com.sparta.teamssc.domain.user.user.entity.User;
import com.sparta.teamssc.domain.user.user.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CommentServiceImpl implements CommentService {

    private final CommentRepository commentRepository;
    private final BoardService boardService;
    private final UserService userService;

    // 댓글 생성
    @Override
    public void createComment(Long boardId, CommentRequestDto commentRequestDto, String username) {
        try {
            Board board = boardService.findBoardByBoardId(boardId);
            User user = userService.findByUsername(username);

            Comment comment = Comment.builder()
                    .content(commentRequestDto.getContent())
                    .parentCommentId(commentRequestDto.getParentCommentId())
                    .user(user)
                    .board(board)
                    .build();

            commentRepository.save(comment);
        } catch (Exception e) {
            throw new IllegalArgumentException("댓글 생성을 실패했습니다.");
        }
    }

    // 특정 게시글에 있는 댓글 조회
    @Override
    public Page<CommentResponseDto> getCommentFromBoard(Long boardId, int page) {

        Pageable pageable = PageRequest.of(page, 5, Sort.by(Sort.Direction.DESC, "createAt"));

        Page<CommentResponseDto> commentPage = commentRepository.findPagedParentCommentList(boardId, pageable);

        if (commentPage.isEmpty()) {
            throw new IllegalArgumentException("작성된 댓글이 없거니, " + (page + 1) + " 페이지에 댓글이 없습니다.");
        }
        return commentPage;
    }

}
