package com.sparta.teamssc.virtual;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.VirtualThreadTaskExecutor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 가상 스레드 도입 전 → 후 수치 비교 테스트
 *
 * 리팩토링 전: RabbitMQ Consumer가 플랫폼 스레드 풀(기본 5개)에서 실행
 * 리팩토링 후: VirtualThreadTaskExecutor → 작업당 가상 스레드 생성
 */
class VirtualThreadThroughputTest {

    private static final int TASK_COUNT = 500;
    private static final int SLEEP_MS = 10; // I/O 대기 시뮬레이션

    // ───────────────────────────────────────────────────────────────────────
    // 가상 스레드 기본 검증
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("VirtualThreadTaskExecutor 실행 스레드가 가상 스레드임을 확인")
    void 가상스레드_확인() throws InterruptedException {
        VirtualThreadTaskExecutor executor = new VirtualThreadTaskExecutor("rabbitmq-consumer-");
        AtomicBoolean isVirtual = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        executor.execute(() -> {
            isVirtual.set(Thread.currentThread().isVirtual());
            assertThat(Thread.currentThread().getName()).startsWith("rabbitmq-consumer-");
            latch.countDown();
        });

        latch.await();
        assertThat(isVirtual.get()).isTrue();
        System.out.println("[검증] RabbitMQ Consumer 실행 스레드 → 가상 스레드 확인");
    }

    // ───────────────────────────────────────────────────────────────────────
    // 처리량 비교: 플랫폼 스레드 풀 vs 가상 스레드
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[수치] 플랫폼 스레드 5개 vs 가상 스레드 — 500개 작업 처리 시간 비교")
    void 처리량_비교_5개_플랫폼_vs_가상() throws InterruptedException {
        // 리팩토링 전: RabbitMQ 기본 Consumer 스레드 5개로 처리
        long platformTime = measureTime(Executors.newFixedThreadPool(5), TASK_COUNT);

        // 리팩토링 후: 가상 스레드 (작업당 1개)
        long virtualTime = measureTime(Executors.newVirtualThreadPerTaskExecutor(), TASK_COUNT);

        System.out.printf("%n[수치] %d개 작업, I/O %dms 시뮬레이션%n", TASK_COUNT, SLEEP_MS);
        System.out.printf("  리팩토링 전 (플랫폼 스레드 5개): %4d ms%n", platformTime);
        System.out.printf("  리팩토링 후 (가상 스레드):        %4d ms%n", virtualTime);
        System.out.printf("  개선율: %.1f배 빠름%n", (double) platformTime / virtualTime);

        assertThat(virtualTime).isLessThan(platformTime);
    }

    @Test
    @DisplayName("[수치] 플랫폼 스레드 10개 vs 가상 스레드 — 500개 작업 처리 시간 비교")
    void 처리량_비교_10개_플랫폼_vs_가상() throws InterruptedException {
        long platformTime = measureTime(Executors.newFixedThreadPool(10), TASK_COUNT);
        long virtualTime = measureTime(Executors.newVirtualThreadPerTaskExecutor(), TASK_COUNT);

        System.out.printf("%n[수치] %d개 작업, I/O %dms 시뮬레이션%n", TASK_COUNT, SLEEP_MS);
        System.out.printf("  리팩토링 전 (플랫폼 스레드 10개): %4d ms%n", platformTime);
        System.out.printf("  리팩토링 후 (가상 스레드):         %4d ms%n", virtualTime);
        System.out.printf("  개선율: %.1f배 빠름%n", (double) platformTime / virtualTime);

        assertThat(virtualTime).isLessThan(platformTime);
    }

    // ───────────────────────────────────────────────────────────────────────
    // 동시 처리 가능 작업 수 비교
    // ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[수치] 동시 실행 가능한 작업 수 — 플랫폼 스레드 10개 vs 가상 스레드")
    void 동시처리_작업수_비교() throws InterruptedException {
        int taskCount = 200;

        // 플랫폼 스레드 10개: 동시에 10개만 실행, 나머지 대기
        AtomicInteger platformPeakConcurrency = new AtomicInteger(0);
        AtomicInteger platformCurrent = new AtomicInteger(0);
        long platformTime = measureConcurrency(
                Executors.newFixedThreadPool(10), taskCount,
                platformCurrent, platformPeakConcurrency
        );

        // 가상 스레드: 동시에 200개 모두 실행 가능
        AtomicInteger virtualPeakConcurrency = new AtomicInteger(0);
        AtomicInteger virtualCurrent = new AtomicInteger(0);
        long virtualTime = measureConcurrency(
                Executors.newVirtualThreadPerTaskExecutor(), taskCount,
                virtualCurrent, virtualPeakConcurrency
        );

        System.out.printf("%n[수치] %d개 작업 동시 실행%n", taskCount);
        System.out.printf("  플랫폼 스레드 10개: 최대 동시 처리 ~10개, 소요 %4d ms%n", platformTime);
        System.out.printf("  가상 스레드:        최대 동시 처리 ~%d개, 소요 %4d ms%n",
                virtualPeakConcurrency.get(), virtualTime);

        assertThat(virtualTime).isLessThan(platformTime);
    }

    // ───────────────────────────────────────────────────────────────────────
    // 헬퍼
    // ───────────────────────────────────────────────────────────────────────

    private long measureTime(ExecutorService executor, int taskCount) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(taskCount);
        long start = System.currentTimeMillis();

        for (int i = 0; i < taskCount; i++) {
            executor.submit(() -> {
                try {
                    Thread.sleep(SLEEP_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        return System.currentTimeMillis() - start;
    }

    private long measureConcurrency(ExecutorService executor, int taskCount,
                                    AtomicInteger current, AtomicInteger peak) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(taskCount);
        long start = System.currentTimeMillis();

        for (int i = 0; i < taskCount; i++) {
            executor.submit(() -> {
                int c = current.incrementAndGet();
                peak.updateAndGet(p -> Math.max(p, c));
                try {
                    Thread.sleep(SLEEP_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    current.decrementAndGet();
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        return System.currentTimeMillis() - start;
    }
}
