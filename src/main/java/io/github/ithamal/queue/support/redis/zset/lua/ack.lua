local bucketKey = KEYS[1]
local inboundKey = KEYS[2]
local outboundKey = KEYS[3]
local consumeGroupPattern = KEYS[4]
local consumeNumKey = KEYS[5]
local deadKey = KEYS[6]
local archiveKey = KEYS[7]
local isDelete = tonumber(ARGV[1])
local isArchive = tonumber(ARGV[2])
local element = ARGV[3]

local consumeGroupKeys = redis.call('keys', consumeGroupPattern)

redis.call('zrem', outboundKey, element)
redis.call('hdel', consumeNumKey, element)
redis.call('hdel', deadKey, element)

local score = tonumber(redis.call('zscore', inboundKey, element))

if (score == nil) then
    return 0
end

-- 是否允许删除
local allowDelete = true
for i = 1, #consumeGroupKeys do
    local consumeGroupKey = consumeGroupKeys[i]
    local offset = redis.call('hget', consumeGroupKey, 'offset')
    if(tonumber(offset) < score) then
        allowDelete = false;
        break
    end
end

-- 允许删除
if (isDelete == 1 and allowDelete == true) then
    -- 归档
    if(isArchive == 1) then
        local value = redis.call('hget', bucketKey, element)
        redis.call('hset', archiveKey, element, value)
    end
    -- 删除
    redis.call('zrem', inboundKey, element)
    redis.call('hdel', bucketKey, element)
end

return score