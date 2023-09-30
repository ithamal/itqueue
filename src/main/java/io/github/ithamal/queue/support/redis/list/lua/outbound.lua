local inboundKey = KEYS[1]
local outboundKey = KEYS[2]
local size = tonumber(ARGV[1])
local time = tonumber(ARGV[2])
local elements = {}

for i = 1, size do
    local element = redis.call('rpop', inboundKey)
    if element then
        redis.call('ZADD', outboundKey, time, element)
        table.insert(elements, element)
    else
        break
    end
end

return elements;
