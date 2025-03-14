package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long expireTime, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), expireTime, timeUnit);
    }

    public void setWithLocalExpire(String key, Object value, Long expireTime, TimeUnit timeUnit) {
        RedisData redisData = RedisData.builder()
                                        .data(value)
                                        .expireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(expireTime)))
                                        .build();
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <T,ID> T querywithPassThrough(
            String keyPrefix, ID id , Class<T> clazz, Function<ID,T> function,Long expireTime, TimeUnit timeUnit) {
        String key = keyPrefix + id;
        // 从Redis查缓存
        String Json = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 判断是否存在
        if(StrUtil.isNotBlank(Json)){
            // 存在
            return JSONUtil.toBean(Json, clazz);
        }
        // 命中的是否是空值
        if (Json != null) {
            return null;
        }
        // 不存在 根据id查数据库
        T t = function.apply(id);
        // 不存在，返回错误
        if(t == null){
            // 空值写入
            stringRedisTemplate.opsForValue().set(key,"", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 存在，吸入Redis
        this.set(key, t, expireTime, timeUnit);
        return t;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <T,ID> T querywithLocalExpire(String keyPrefix,ID id,Class<T> clazz, Function<ID,T> function,Long expireTime, TimeUnit timeUnit,String keyLock) {
        String key = keyPrefix + id;
        // 从Redis查缓存
        String Json = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 判断是否存在
        if(StrUtil.isBlank(Json)){
            // 不存在
            return null;
        }
        // 命中，需要先json反序列化为对象
        RedisData redisData = JSONUtil.toBean(Json,RedisData.class);
        LocalDateTime Time = redisData.getExpireTime();
        T t = JSONUtil.toBean((JSONObject) redisData.getData(), clazz);
        // 判断是否过期
        if(Time.isAfter(LocalDateTime.now())){
            // 未过期，直接返回店铺信息
            return t;
        }
        // 过期
        // 缓存重建
        // 获取互斥锁
        String shopLock = keyLock + id;
        boolean isLock = trylock(shopLock);
        // 判断是否获取成功
        if(isLock){
            // 检查
            Json = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
            if (StrUtil.isNotBlank(Json)) {
                redisData = JSONUtil.toBean(Json,RedisData.class);
                Time = redisData.getExpireTime();
                if (Time.isAfter(LocalDateTime.now())) {
                    // 缓存已经被其他线程重建，不再过期，直接返回店铺信息
                    return JSONUtil.toBean((JSONObject) redisData.getData(), clazz);
                }
            }
            // 缓存仍然过期，开启新的线程，重构缓存
            // 成功，开启新的线程，重构缓存
            CACHE_REBUILD_EXECUTOR.execute(() -> {
                try {
                    T t1 = function.apply(id);
                    this.setWithLocalExpire(key, t1, expireTime, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unlock(shopLock);
                }
            });
        }
        // 不成功，返回过期信息
        return t;
    }

    private boolean trylock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + key);
    }

}
