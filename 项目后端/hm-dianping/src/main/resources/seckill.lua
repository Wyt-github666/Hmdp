--1 参数列表
--1.1 优惠卷id
local voucherId = ARGV[1]
--1.2 用户id
local userId = ARGV[2]
--1.3 订单id
local orderId = ARGV[3]

local stockKey = 'seckill:stock:' .. voucherId

local orderKey = 'seckill:order:' .. voucherId

if(tonumber(redis.call('get',stockKey)) <= 0) then
    return 1

end

if(redis.call('sismember',orderKey,userId) == 1) then
    return 2
end

redis.call("incrby",stockKey,-1)

redis.call("sadd",orderKey,userId)
-- 发消息
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)
return 0
