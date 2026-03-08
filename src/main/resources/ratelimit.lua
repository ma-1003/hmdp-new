-- 令牌桶限流 Lua 脚本
-- KEYS[1]: 令牌桶的 Redis key
-- ARGV[1]: 桶容量（最大令牌数）
-- ARGV[2]: 每秒生成的令牌数
-- ARGV[3]: 当前时间戳（毫秒）
-- ARGV[4]: 本次请求需要的令牌数

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

-- 获取当前桶状态
local bucket = redis.call('hmget', key, 'tokens', 'last_time')
local tokens = tonumber(bucket[1])
local last_time = tonumber(bucket[2])

-- 首次访问，初始化满桶
if tokens == nil then
    tokens = capacity
    last_time = now
end

-- 计算从上次到现在应补充的令牌数
local delta = math.max(0, now - last_time)
local new_tokens = math.min(capacity, tokens + delta * rate / 1000)

-- 判断令牌是否足够
if new_tokens >= requested then
    new_tokens = new_tokens - requested
    redis.call('hset', key, 'tokens', new_tokens, 'last_time', now)
    redis.call('expire', key, math.ceil(capacity / rate) + 1)
    return 1
else
    -- 令牌不足，仍更新时间戳和令牌数（已补充的不丢失）
    redis.call('hset', key, 'tokens', new_tokens, 'last_time', now)
    redis.call('expire', key, math.ceil(capacity / rate) + 1)
    return 0
end
