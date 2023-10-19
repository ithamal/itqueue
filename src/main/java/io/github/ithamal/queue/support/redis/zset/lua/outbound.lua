local inboundKey = KEYS[1]
local outboundKey = KEYS[2]
local consumeGroupKey = KEYS[3]
local consumeNumKey = KEYS[4]
local deadKey = KEYS[5]
local size = tonumber(ARGV[1])
local time = tonumber(ARGV[2])
local retryLaterList = {}
for i, value in pairs(ARGV) do
    if i > 2  then
        retryLaterList[i - 2] = tonumber(value)
    end
end

local score = redis.call('hget', consumeGroupKey, 'offset')
if score == false then
    score = 0
end

local elements  = {}

-- 返回结果，并加入到出栈集合
local results  = {}

-- 超时重试
if size > 0 then
    elements = redis.call('zrangebyscore', outboundKey, 0 , time ,  'limit', 0, size)
    for i = 1, #elements do
        local element = elements[i]
        table.insert(results, element)
        end
    end

-- 出栈（记录消费组偏移量）
size = size - #elements
if size > 0 then
    elements = redis.call('zrangebyscore', inboundKey,  '('..score , '+inf', 'withscores', 'limit', 0, size)
    if #elements > 1 then
        score = elements[#elements]
        redis.call('hmset', consumeGroupKey, 'offset', score, 'time', time)
    end
    for i = 1, #elements do
        if i % 2 == 1  then
            local element = elements[i]
            table.insert(results, element)
        end
    end
end

for i = 1, #results do
    local element = results[i]
    -- 增加消费次数
    local consumeNum = redis.call('hincrby', consumeNumKey, element, 1)
    local retryLater = retryLaterList[consumeNum]
    if retryLater == nil then
        redis.call('zrem' , outboundKey,  element)
        redis.call('hdel' , consumeNumKey,  element)
        redis.call('hset' , deadKey,  element, consumeNum)
    else
        -- 进入出栈队列（存在则更新）
        local next_time = time + retryLater;
        redis.call('zadd' , outboundKey, next_time, element)
    end
end

return results
