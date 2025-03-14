package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result gettypeList() {
        String shopTypeJsonStr = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_TYPE_TTL);
        if(StrUtil.isNotBlank(shopTypeJsonStr)){
            List<ShopType> typeList = JSONUtil.toList(shopTypeJsonStr, ShopType.class);
            return Result.ok(typeList);
        }
        List<ShopType> typeList = query().orderByAsc("sort").list();
        if(typeList == null || typeList.isEmpty()){
            return Result.fail("服务器错误");
        }
        String JsonStr = JSONUtil.toJsonStr(typeList);
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_TYPE_TTL,JsonStr);
        return Result.ok(JsonStr);
    }
}
