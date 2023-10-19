local bucketKey = KEYS[1]
local inboundKey = KEYS[2]
local msgId = KEYS[3]
local value = ARGV[1]

redis.call('hset', bucketKey, msgId, value)

redis.call('lpush', inboundKey, msgId)

return 1
