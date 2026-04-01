package com.sparta.teamssc.benchmark;

import com.sparta.teamssc.domain.chat.entity.Message;
import com.sparta.teamssc.domain.chat.entity.RoomType;
import com.sparta.teamssc.domain.chat.repository.MessageRepository;
import com.sparta.teamssc.common.config.QueryDSLConfig;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FixedThreadPool-200 vs VirtualThread 성능 비교
 *
 * 목적: TeamSSC 채팅 메시지 저장 흐름(existsByMessageId → INSERT)에서
 *       두 스레드 모델의 처리량·응답시간·리소스 사용량을 실제 MySQL로 측정
 *
 * 환경
 *   - Spring Boot 3.3.2 / Java 21
 *   - DB: MySQL (application-local.yml 기준)
 *   - HikariCP 커넥션 풀: 10개 고정
 *   - Thread.sleep() 없음 — 실제 DB I/O만 측정
 *
 * 실행 방법 (IntelliJ)
 *   1. 로컬 MySQL 실행 확인
 *   2. Run > Edit Configurations > VM options: -Dspring.profiles.active=local
 *   3. 클래스 우클릭 → Run 'ThreadModelBenchmarkTest'
 *   4. 전체 소요 약 3~5분
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("local")
@Import(QueryDSLConfig.class)
@TestPropertySource(properties = {
        "spring.datasource.hikari.maximum-pool-size=10",
        "spring.datasource.hikari.minimum-idle=10",
        "spring.jpa.show-sql=false"           // 콘솔 노이즈 제거
})
@Transactional(propagation = Propagation.NOT_SUPPORTED)  // 테스트 트랜잭션 비활성화 → 실제 커밋
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ThreadModelBenchmarkTest {

    @Autowired
    private MessageRepository messageRepository;

    // ─── 측정 설정 ────────────────────────────────────────────────
    private static final int[] CONCURRENCY_LEVELS = {50, 100, 200, 300, 500};
    private static final int ITERATIONS = 3;    // 케이스당 3회 반복 후 평균
    private static final int TIMEOUT_SEC = 60;  // 배치당 최대 대기

    // ─── JVM 모니터링 ─────────────────────────────────────────────
    private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    // ─── 결과 누적 ─────────────────────────────────────────────────
    private static final List<String> resultRows = new ArrayList<>();

    // ══════════════════════════════════════════════════════════════
    //  라이프사이클
    // ══════════════════════════════════════════════════════════════

    @BeforeEach
    void setUp() {
        messageRepository.deleteAll();
    }

    @AfterEach
    void tearDown() {
        messageRepository.deleteAll();
    }

    @AfterAll
    static void printResultTable() {
        String sep = "─".repeat(98);
        System.out.println("\n" + "═".repeat(98));
        System.out.printf("%-8s  %-18s  %8s  %8s  %8s  %7s  %10s  %10s%n",
                "동시요청", "방식", "avg(ms)", "p95(ms)", "req/s", "에러율%", "피크스레드", "메모리MB");
        System.out.println(sep);
        resultRows.forEach(System.out::println);
        System.out.println("═".repeat(98));
    }

    // ══════════════════════════════════════════════════════════════
    //  메인 벤치마크
    // ══════════════════════════════════════════════════════════════

    @Test
    @Order(1)
    @DisplayName("FixedThreadPool-200 vs VirtualThread — 동시요청 50·100·200·300·500 처리 성능 비교")
    void 스레드_모델_성능_비교() throws Exception {

        // ── 워밍업 1회 (JIT 컴파일 안정화 후 측정) ──────────────────
        System.out.println("\n▶ [워밍업] FixedThreadPool-200 / 50 VU — 결과 미수집");
        runBatch(Executors.newFixedThreadPool(200), 50);
        messageRepository.deleteAll();
        System.out.println("  완료. 본 측정 시작.\n");

        // ── 본 측정 ───────────────────────────────────────────────
        for (int concurrency : CONCURRENCY_LEVELS) {
            System.out.printf("▶ [측정] %d VU%n", concurrency);

            // Case A: FixedThreadPool-200 (platform thread)
            double[] fixedSum = new double[6];
            for (int iter = 1; iter <= ITERATIONS; iter++) {
                double[] m = runBatch(Executors.newFixedThreadPool(200), concurrency);
                accumulate(fixedSum, m);
                System.out.printf("   Fixed  %d/%d → avg=%.1fms  req/s=%.1f%n",
                        iter, ITERATIONS, m[0], m[2]);
                messageRepository.deleteAll();
            }
            double[] fixedAvg = average(fixedSum);

            // Case B: VirtualThread (virtual thread per task)
            double[] virtualSum = new double[6];
            for (int iter = 1; iter <= ITERATIONS; iter++) {
                double[] m = runBatch(Executors.newVirtualThreadPerTaskExecutor(), concurrency);
                accumulate(virtualSum, m);
                System.out.printf("   Virtual %d/%d → avg=%.1fms  req/s=%.1f%n",
                        iter, ITERATIONS, m[0], m[2]);
                messageRepository.deleteAll();
            }
            double[] virtualAvg = average(virtualSum);

            resultRows.add(row(concurrency, "FixedPool-200", fixedAvg));
            resultRows.add(row(concurrency, "VirtualThread", virtualAvg));
            resultRows.add("─".repeat(98));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  측정 엔진
    // ══════════════════════════════════════════════════════════════

    /**
     * concurrency개 요청을 동시에 실행하고 메트릭 6개를 반환한다.
     *
     * 반환 배열 인덱스:
     *   [0] avg 응답시간 (ms)
     *   [1] p95 응답시간 (ms)
     *   [2] 초당 처리량 (req/s)
     *   [3] 에러율 (%)
     *   [4] 피크 스레드 수 (JVM 전체)
     *   [5] 힙 메모리 증가량 (MB)
     *
     * 측정 대상 작업:
     *   existsByMessageId() 조회 → messages 테이블 INSERT → (ACK 단계 생략)
     */
    private double[] runBatch(ExecutorService executor, int concurrency) throws Exception {
        // GC로 베이스라인 안정화
        System.gc();
        long heapBefore = memoryMXBean.getHeapMemoryUsage().getUsed();
        threadMXBean.resetPeakThreadCount();

        long[] responseTimes = new long[concurrency];   // 각 요청의 응답시간
        AtomicInteger errorCount = new AtomicInteger(0);

        // 모든 스레드를 동시에 출발시키기 위한 게이트
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);

        // ── 태스크 제출 ──────────────────────────────────────────
        for (int i = 0; i < concurrency; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    startGate.await();  // 게이트 열릴 때까지 대기 → 동시 출발

                    long start = System.currentTimeMillis();

                    // 측정 대상: existsByMessageId() + save()
                    // (MessageListener.handleMessage 핵심 경로와 동일)
                    Message msg = Message.builder()
                            .content("benchmark-message-" + idx)
                            .sender("bench-user-" + idx)
                            .roomId(1L)
                            .roomType(RoomType.TEAM)
                            .build();

                    if (!messageRepository.existsByMessageId(msg.getMessageId())) {
                        messageRepository.save(msg);
                    }

                    responseTimes[idx] = System.currentTimeMillis() - start;

                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    responseTimes[idx] = -1;  // 에러 표시
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // ── 동시 출발 후 완료 대기 ───────────────────────────────
        long wallStart = System.currentTimeMillis();
        startGate.countDown();
        doneLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS);
        long wallMs = System.currentTimeMillis() - wallStart;

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // ── 메트릭 계산 ──────────────────────────────────────────
        long heapAfter = memoryMXBean.getHeapMemoryUsage().getUsed();
        int peakThreads = threadMXBean.getPeakThreadCount();

        long[] valid = Arrays.stream(responseTimes)
                .filter(t -> t >= 0)
                .sorted()
                .toArray();

        double avg    = valid.length > 0 ? Arrays.stream(valid).average().orElse(0) : 0;
        double p95    = valid.length > 0 ? valid[Math.min((int)(valid.length * 0.95), valid.length - 1)] : 0;
        int    success = concurrency - errorCount.get();
        double reqPerSec = wallMs > 0 ? success * 1000.0 / wallMs : 0;
        double errorRate = (double) errorCount.get() / concurrency * 100;
        double heapMB = Math.max((heapAfter - heapBefore) / (1024.0 * 1024.0), 0);

        return new double[]{avg, p95, reqPerSec, errorRate, peakThreads, heapMB};
    }

    // ══════════════════════════════════════════════════════════════
    //  유틸리티
    // ══════════════════════════════════════════════════════════════

    private void accumulate(double[] sum, double[] values) {
        for (int i = 0; i < sum.length; i++) sum[i] += values[i];
    }

    private double[] average(double[] sum) {
        return Arrays.stream(sum).map(v -> v / ITERATIONS).toArray();
    }

    private String row(int concurrency, String mode, double[] m) {
        return String.format("%-8d  %-18s  %8.1f  %8.1f  %8.1f  %7.2f  %10.0f  %10.1f",
                concurrency, mode,
                m[0],  // avg
                m[1],  // p95
                m[2],  // req/s
                m[3],  // 에러율
                m[4],  // 피크 스레드
                m[5]); // 메모리 MB
    }
}
