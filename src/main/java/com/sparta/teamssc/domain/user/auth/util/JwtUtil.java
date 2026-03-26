package com.sparta.teamssc.domain.user.auth.util;


import com.sparta.teamssc.domain.user.auth.config.JwtConfig;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class JwtUtil {
    private static String secretKey;
    private static long accessTokenExpiration;
    private static long refreshTokenExpiration;

    private JwtUtil(){}

    public static void init(JwtConfig jwtConfig) {
        secretKey = Base64.getEncoder().encodeToString(jwtConfig.getSecretKey().getBytes());
        accessTokenExpiration = jwtConfig.getAccessTokenExpiration();
        refreshTokenExpiration = jwtConfig.getRefreshTokenExpiration();
    }

    /**
     * 사용자 이름으로 엑세스 토큰 발급
     *
     * @param username
     * @return
     */
    public static String createAccessToken(String username, List<String> roles) {
        return generateToken(username, accessTokenExpiration, roles);
    }

    /**
     * 사용자 이름으로 엑세스 토큰 발급
     *
     * @param username
     * @return
     */
    public static String createRefreshToken(String username, List<String> roles) {
        return generateToken(username, refreshTokenExpiration, roles);
    }


    /**
     * 토큰 생성 내부 메서드
     *
     * @param username
     * @param expiration
     * @return
     */
    public static String generateToken(String username, long expiration, List<String> roles) {
        return Jwts.builder()
                .setSubject(username) // 토큰 주체
                .claim("roles", roles)
                .setIssuedAt(new Date()) // 토큰 발행 시간
                .setExpiration(new Date(System.currentTimeMillis() + expiration)) // 토큰 만료시간
                .signWith(SignatureAlgorithm.HS256, secretKey)
                .compact();
    }

    /**
     * JWT 토큰에서 Claims을 추출
     *
     * @param token JWT 토큰
     * @return 토큰에서 추출한 Claims 객체
     */
    public static Claims extractClaims(String token) {
        return Jwts.parser()
                .setSigningKey(secretKey)
                .parseClaimsJws(token)
                .getBody();
    }

    // 토큰유효성 검사
    public static boolean validateToken(String token, UserDetails userDetails) {
        // 토큰에서 사용자 이름 추출
        final String username = getUsernameFromToken(token);
        // 토큰 검사(이름 일치, 만료 확인)
        return (username.equals(userDetails.getUsername()) && !isTokenExpired(token));
    }

    // 리프레시토큰 유효성 검사
    public static boolean validateRefreshToken(String token) {
        return validateToken(token);
    }

    // 토큰 유효성 검사
    public static boolean validateToken(String token) {
        try {
            extractClaims(token);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 토큰 만료일 claim 추출
     *
     * @param token JWT 토큰
     * @return 현재 시간
     */
    private static boolean isTokenExpired(String token) {
        final Date expiration = extractClaims(token).getExpiration();
        return expiration.before(new Date());
    }

    // 사용자 추출
    public static String getUsernameFromToken(String token) {
        return extractClaims(token).getSubject();
    }

    public static List<SimpleGrantedAuthority> getRolesFromToken(String token) {
        Claims claims = extractClaims(token);
        List<String> roles = claims.get("roles", List.class);
        return roles.stream()
                .map(role -> new SimpleGrantedAuthority("ROLE_" + role)) // ROLE_ 접두사 추가
                .collect(Collectors.toList());
    }
}
