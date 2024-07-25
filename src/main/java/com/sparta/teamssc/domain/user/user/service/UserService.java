package com.sparta.teamssc.domain.user.user.service;

import com.sparta.teamssc.domain.user.auth.dto.request.SignupRequestDto;
import com.sparta.teamssc.domain.user.auth.dto.request.LoginRequestDto;
import com.sparta.teamssc.domain.user.auth.dto.response.LoginResponseDto;
import com.sparta.teamssc.domain.user.user.entity.User;

public interface UserService {
    // 회원가입
    void signup(SignupRequestDto signupRequestDto);

    // 로그인
    LoginResponseDto login(LoginRequestDto loginRequestDto);

    // 이메일로 사용자가져오기
    User getUserByEmail(String email);

    User findByUsername(String email);

    void logout(String email);

    LoginResponseDto tokenRefresh(String refreshToken);

    void updateUser(User user);

    void withdrawn(String email);

    User findById(Long id);
}