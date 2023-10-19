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

redis.call('zrem', outboundKey, element)
redis.call('hdel', consumeNumKey, element)
redis.call('hdel', deadKey, element)

-- 归档
if (isDelete == 1 and isArchive == 1) then
    local value = redis.call('hget', bucketKey, element)
    redis.call('hset', archiveKey, element, value)
end

-- 删除
if (isDelete == 1) then
    redis.call('hdel', bucketKey, element)
end

return 1