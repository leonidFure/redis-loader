package com.redisloader.scheduling;

import com.redisloader.service.RedisService;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.IntervalTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Configuration
@EnableScheduling
@EnableConfigurationProperties(RedisLoadProperties.class)
public class RedisLoadScheduler implements SchedulingConfigurer {

    private final RedisService redisService;
    private final RedisLoadProperties properties;
    private final byte[] payload;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Disposable> subscription = new AtomicReference<>();

    public RedisLoadScheduler(final RedisService redisService, final RedisLoadProperties properties) {
        this.redisService = redisService;
        this.properties = properties;
        this.payload = new byte[properties.getSize()];
        ThreadLocalRandom.current().nextBytes(this.payload);
    }

    @Override
    public void configureTasks(final ScheduledTaskRegistrar taskRegistrar) {
        final var interval = this.properties.getInterval();
        taskRegistrar.addFixedRateTask(new IntervalTask(this::load, interval, Duration.ZERO));
    }

    @PreDestroy
    public void shutdown() {
        final Disposable current = this.subscription.getAndSet(null);
        if (current != null) {
            current.dispose();
        }
    }

    private void load() {
        if (!this.running.compareAndSet(false, true)) {
            log.warn("Skip Redis load because the previous run is still active");
            return;
        }
        final int totalCount = this.properties.getTotalCount();
        final int concurrency = this.properties.getConcurrency();
        final Duration delay = this.properties.getDelay();
        if (concurrency < 1) {
            this.running.set(false);
            log.error("Redis load concurrency must be positive: {}", concurrency);
            return;
        }
        log.info(
                "Starting Redis load: totalCount={}, size={}, concurrency={}, delay={}",
                totalCount,
                this.properties.getSize(),
                concurrency,
                delay
        );
        final Disposable disposable = this.writeAll(totalCount, concurrency, delay)
                .doFinally(signal -> this.running.set(false))
                .subscribe(
                        null,
                        error -> log.error("Redis load failed", error),
                        () -> log.info("Redis load finished: totalCount={}", totalCount)
                );
        this.subscription.set(disposable);
    }

    private Mono<Void> writeAll(final int totalCount, final int concurrency, final Duration delay) {
        return Flux.range(0, totalCount)
                .buffer(concurrency)
                .concatMap(batch -> Flux.fromIterable(batch)
                        .flatMap(this::write, concurrency)
                        .then(Mono.delay(delay))
                        .then())
                .then()
                .subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Boolean> write(final int index) {
        return this.redisService.putWithTtl(key(index), this.payload);
    }

    private static byte[] key(final int index) {
        return Integer.toString(index).getBytes(StandardCharsets.UTF_8);
    }
}
