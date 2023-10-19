local bucketKey = KEYS[1]
local inboundKey = KEYS[2]
local msgId = KEYS[3]
local value = ARGV[1]
local score = tonumber(ARGV[2])

redis.call('hset', bucketKey, msgId, value)

redis.call('zadd', inboundKey, score, msgId)

return score
