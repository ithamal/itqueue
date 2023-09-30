local outboundKey = KEYS[1]
local deadKey = KEYS[2]
local tryNumKey = KEYS[3]

redis.call('hdel', tryNumKey, unpack(ARGV))

local result = redis.call('zrem', outboundKey, unpack(ARGV))

for i, bucketKey in pairs(KEYS) do
    if  i > 3 then
        for i, field in pairs(ARGV) do
            local value = redis.call('hget', bucketKey, field)
            redis.call('hset', deadKey, field, value)
            redis.call('hdel', bucketKey, field)
        end
    end
end

return result;
