package com.hmdp.config;

import com.hmdp.utils.RedisConstants;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfig {

    @Bean
    public RedissonClient redisson() {
        Config config = new Config();
        config.useSingleServer().setAddress(RedisConstants.REDIS + "://" + RedisConstants.REDIS_HOST + ":" + RedisConstants.REDIS_PORT)
                .setDatabase(2);
        return Redisson.create(config);
    }

}
