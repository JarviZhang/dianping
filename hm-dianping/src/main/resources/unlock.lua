-- 锁的key
-- local key = KEY[1]
-- 当前线程标识
-- local threadId = ARGV[1]
-- 获取锁中的线程标识 get key
local id = redis.call('get', KEY[1])
-- 比较线程标识和所种标识是否相等
if(id == ARGV[1]) then
	-- 释放锁
	return redis.call('del',KEY[1])
end
return 0