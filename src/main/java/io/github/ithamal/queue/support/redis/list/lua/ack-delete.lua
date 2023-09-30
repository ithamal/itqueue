local outboundKey = KEYS[1]
local archiveKey = KEYS[2]
local tryNumKey = KEYS[3]

redis.call('hdel', tryNumKey, unpack(ARGV))

local result = redis.call('zrem', outboundKey, unpack(ARGV))

for i, bucketKey in pairs(KEYS) do
    if  i > 3 then
        redis.call('hdel', bucketKey, unpack(ARGV))
    end
end


return result;
