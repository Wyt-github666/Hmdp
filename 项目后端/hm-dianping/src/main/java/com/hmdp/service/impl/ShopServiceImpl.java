package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.RedisData;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private CacheClient cacheClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        // Shop shop = querywithPassThrough(id);
        // Shop shop = cacheClient.querywithPassThrough(RedisConstants.CACHE_SHOP_KEY,id,Shop.class,this::getById,RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);

        // 缓存穿透（互斥锁）
        Shop shop = querywithMutex(id);
//        if (shop == null) {
//            return Result.fail("店铺不存在");
//        }
//        return Result.ok(shop);

        // 缓存穿透（逻辑过期）
        // Shop shop = querywithLocalExpire(id);
//        Shop shop = cacheClient.querywithLocalExpire(
//                RedisConstants.CACHE_SHOP_KEY,id,Shop.class,this::getById,20L,TimeUnit.SECONDS,RedisConstants.LOCK_SHOP_KEY);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id为空");
        }
        updateById(shop);
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    private boolean trylock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + key);
    }

    public Shop querywithPassThrough(Long id){
        // 从Redis查缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            // 存在
            return  JSONUtil.toBean(shopJson, Shop.class);
        }
        // 命中的是否是空值
        if (shopJson != null) {
            return null;
        }
        // 不存在 根据id查数据库
        Shop shop = getById(id);
        // 不存在，返回错误
        if(shop == null){
            // 空值写入
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,"", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 存在，吸入Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    private Shop querywithMutex(Long id){
        // 从Redis查缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            // 存在
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 命中的是否是空值
        if (shopJson != null) {
            return null;
        }
        // 获取锁
        String shopLock = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = trylock(shopLock);
            if(!isLock){
                // 获取锁失败
                Thread.sleep(50);
                return querywithMutex(id);
            }
            // 获取锁成功
            // 不存在 根据id查数据库
            shop = getById(id);
            // 不存在，返回错误
            if(shop == null){
                // 空值写入
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,"", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 存在，吸入Redis
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unlock(shopLock);
        }
        // 释放锁
        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private Shop querywithLocalExpire(Long id){
        // 从Redis查缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 判断是否存在
        if(StrUtil.isBlank(shopJson)){
            // 不存在
            return null;
        }
        // 命中，需要先json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        // 判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            // 未过期，直接返回店铺信息
            return shop;
        }
        // 过期
        // 缓存重建
        // 获取互斥锁
        String shopLock = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = trylock(shopLock);
        // 判断是否获取成功
        if(isLock){
            // 检查
            shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
            if (StrUtil.isNotBlank(shopJson)) {
                redisData = JSONUtil.toBean(shopJson, RedisData.class);
                expireTime = redisData.getExpireTime();
                if (expireTime.isAfter(LocalDateTime.now())) {
                    // 缓存已经被其他线程重建，不再过期，直接返回店铺信息
                    return JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
                }
            }
            // 缓存仍然过期，开启新的线程，重构缓存
            // 成功，开启新的线程，重构缓存
            CACHE_REBUILD_EXECUTOR.execute(() -> {
                try {
                    saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unlock(shopLock);
                }
            });
        }
        // 不成功，返回过期信息
        return shop;
    }

    public void saveShop2Redis(Long id, Long exporeSceonds){
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(exporeSceonds));
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        if (x == null || y == null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> search = stringRedisTemplate.opsForGeo()
                .radius(key,
                        new Circle(new Point(x,y),5000),
                        RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                                .includeDistance().limit(end));
        if(search == null){
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>();
        Map<String,Distance> distanceMap = new HashMap<>();
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = search.getContent();
        content.stream().skip(from).forEach(item -> {
            String shopId = item.getContent().getName();
            ids.add(Long.valueOf(shopId));
            Distance distance = item.getDistance();
            distanceMap.put(shopId, distance);
        });
        if(ids.isEmpty()){
            return Result.ok();
        }
        String idsStr = StrUtil.join(",",ids);
        List<Shop> shopList = query().in("id", ids).last("ORDER BY FIELD(id," + idsStr + ")").list();
        for (Shop shop : shopList) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shopList);
    }
}
