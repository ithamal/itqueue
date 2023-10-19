local key = tostring(KEYS[1])
local expire = tonumber(ARGV[1])

local number = redis.call('incr', key)

redis.call('expire', key, expire)

return number

