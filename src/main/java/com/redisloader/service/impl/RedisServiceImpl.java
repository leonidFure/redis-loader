package com.redisloader.service.impl;

import com.redisloader.config.RedisServiceProperties;
import com.redisloader.service.RedisService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class RedisServiceImpl implements RedisService {

    private final RedisTemplate<byte[], byte[]> redisTemplate;
    private final RedisServiceProperties redisServiceProperties;

    @Override
    public Mono<Boolean> putWithTtl(final byte[] key, final byte[] value) {
        final var ttl = this.redisServiceProperties.getPutWithTtl();
        return Mono.fromCallable(() -> {
                    this.redisTemplate.opsForValue().set(key, value, ttl);
                    return Boolean.TRUE;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Long> ttl(final byte[] key) {
        return Mono.fromCallable(() -> this.redisTemplate.getExpire(key, TimeUnit.SECONDS))
                .subscribeOn(Schedulers.boundedElastic());
    }
}
