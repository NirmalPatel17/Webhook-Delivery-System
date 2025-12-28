import redis
from dotenv import load_dotenv
import os


redis_client = redis.Redis.from_url(os.getenv("REDIS_URL"))

RATE_LIMIT_SCRIPT = """
-- KEYS[1] = rate limit key (rate_limit:<ip>:<epoch_second>)
-- ARGV[1] = max requests (3)
-- ARGV[2] = window in seconds (1)

local current = redis.call("INCR", KEYS[1])

if current == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[2])
end

if current > tonumber(ARGV[1]) then
    return 0
end

return 1
"""

RATE_LIMIT_SHA = redis_client.script_load(RATE_LIMIT_SCRIPT)
