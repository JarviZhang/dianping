package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import lombok.val;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        //缓存穿透获取的shop
        //Shop shop = queryWithPassThrough(id);

        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
        Shop shop = queryWithLogicalExpire(id);
        if (shop == null){
            return Result.fail("店铺不存在");
        }
        //8. 返回
        return Result.ok(shop);
    }

    //使用逻辑过期方式解决缓存击穿问题
    public Shop queryWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1. 从Redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否命中
        if (StrUtil.isBlank(shopJson)){
            //3. 未命中直接返回null
            return null;
        }
        //4.命中,需要吧json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data,Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5. 判断是否过期

        //
        //8. 返回
        return shop;
    }


    //解决缓存穿透问题的根据id查询店铺
    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1. 从Redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否有数据且不为空
        if (StrUtil.isNotBlank(shopJson)){
            //3. 有数据且不为空,存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //4. 判断是否为空值,解决缓存穿透问题
        //即key = null的情况
        if (shopJson != null){
            return null;
        }
        //5. 不存在根据id查询数据库
        //即不存在key的情况
        Shop shop = getById(id);
        //6. 不存在返回错误
        if (shop == null){
            //将空值写入redis,解决缓存穿透问题
            stringRedisTemplate.opsForValue().set(key, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //7.存在写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //8. 返回
        return shop;
    }

    //根据店铺id获取店铺信息,使用互斥锁解决缓存击穿问题
    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1. 从Redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否存在
        if (StrUtil.isNotBlank(shopJson)){
            //3. 存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //4. 判断是否为空值,解决缓存穿透问题
        if (shopJson != null){
            return null;
        }
        //5. 实现缓存重建
        //5.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try{
            tryLock(lockKey);
            boolean isLock = tryLock(lockKey);
            //5.2 判断是否获取成功
            while(!isLock){
                //5.3 失败则休眠并重试
                Thread.sleep(5);
                isLock = tryLock(lockKey);
            }
            //5.4 成功则double check缓存是否存在
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)){
                //存在直接返回
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            //6. 不存在根据id查询数据库
            shop = getById(id);
            //模拟重建延迟
            //Thread.sleep(200);
            //7. 不存在返回错误
            if (shop == null){
                //将空值写入redis,解决缓存穿透问题
                stringRedisTemplate.opsForValue().set(key, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            //8.存在写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e){
            throw new RuntimeException(e);
        }finally {
            //9. 释放互斥锁
            unLock(lockKey);
        }
        //8. 返回
        return shop;
    }

    //获取锁,用于互斥锁方式解决缓存击穿
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //释放锁,用户互斥锁方式解决缓存击穿
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    //用于向Redis写入带有逻辑过期时间的数据
    public void saveShop2Redis(Long id, Long expireSeconds){
        //1. 查询店铺数据
        Shop shop = getById(id);
        //2. 封装成逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3. 写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空");
        }
        //1. 更新数据库
        updateById(shop);
        //2. 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
