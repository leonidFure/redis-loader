package com.redisloader.scheduling;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "app.scheduler")
public class RedisLoadProperties {

    private Duration interval = Duration.ofMinutes(30);

    private int totalCount = 1000;

    private int size = 1024;

    private int concurrency = 8;

    private Duration delay = Duration.ofMillis(100);
}
