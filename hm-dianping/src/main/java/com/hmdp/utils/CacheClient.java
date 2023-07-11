package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
@Slf4j
public class CacheClient {
    private StringRedisTemplate stringRedisTemplate;

    //逻辑过期方式解决缓存击穿问题的线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //向redis中添加带有过期时间的缓存
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    //向redis中添加带有逻辑过期时间的缓存
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1. 从Redis中查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否有数据且不为空
        if (StrUtil.isNotBlank(json)){
            //3. 有数据且不为空,存在直接返回
            return JSONUtil.toBean(json, type);
        }
        //4. 判断是否为空值,解决缓存穿透问题
        //即key = null的情况
        if (json != null){
            return null;
        }
        //5. 不存在根据id查询数据库
        //即不存在key的情况
        R r = dbFallback.apply(id);
        //6. 不存在返回错误
        if (r == null){
            //将空值写入redis,解决缓存穿透问题
            stringRedisTemplate.opsForValue().set(key, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //7.存在写入redis
        this.set(key, r, time, unit);
        //8. 返回
        return r;
    }

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1. 从Redis中查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否命中
        if (StrUtil.isBlank(json)){
            //3. 未命中直接返回null
            return null;
        }
        //4.命中,需要把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data,type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            //5.1 未过期,返回店铺数据
            return r;
        }
        //6. 已过期,重建缓存
        //6.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //6.2判断是否获取锁成功
        if (isLock){
            //TODO:再次检查是否逻辑超时
            //获取锁成功,开启独立线程,实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key,r1, time, unit);
                } catch (Exception e){
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        //获取锁失败,返回过期的商铺信息
        return r;
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
}
