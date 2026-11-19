package com.redisloader.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "app.redis")
public class RedisServiceProperties {

    /**
     * TTL applied by {@code RedisService#putWithTtl}.
     */
    private Duration putWithTtl = Duration.ofMinutes(5);
}
