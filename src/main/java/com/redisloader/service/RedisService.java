package com.redisloader.service;

import reactor.core.publisher.Mono;

public interface RedisService {

    Mono<Boolean> putWithTtl(byte[] key, byte[] value);

    Mono<Long> ttl(byte[] key);
}
