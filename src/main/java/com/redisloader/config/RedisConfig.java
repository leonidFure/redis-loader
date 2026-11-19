package com.redisloader.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

@Configuration
@EnableConfigurationProperties(RedisServiceProperties.class)
public class RedisConfig {

    @Bean
    RedisTemplate<byte[], byte[]> redisTemplate(final RedisConnectionFactory connectionFactory) {
        final var template = new RedisTemplate<byte[], byte[]>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(RedisSerializer.byteArray());
        template.setValueSerializer(RedisSerializer.byteArray());
        template.setHashKeySerializer(RedisSerializer.byteArray());
        template.setHashValueSerializer(RedisSerializer.byteArray());
        template.afterPropertiesSet();
        return template;
    }
}
