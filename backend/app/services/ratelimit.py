"""Redis token bucket (atomic Lua) + tenant concurrency cap used for fairness."""
from __future__ import annotations

TOKEN_BUCKET_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local rate = tonumber(ARGV[1]); local cap = tonumber(ARGV[2])
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(d[1]); local ts = tonumber(d[2])
if tokens == nil then tokens = cap; ts = now end
tokens = math.min(cap, tokens + (now - ts) * rate)
local allowed = 0; local wait = 0
if tokens >= 1 then tokens = tokens - 1; allowed = 1 else wait = (1 - tokens) / rate end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('EXPIRE', KEYS[1], 120)
return {allowed, tostring(wait)}
"""


async def take_token(rds, endpoint_id, rate_per_sec: float) -> tuple[bool, float]:
    cap = max(rate_per_sec, 1.0)
    allowed, wait = await rds.eval(TOKEN_BUCKET_LUA, 1, f"hr:rl:ep:{endpoint_id}", rate_per_sec, cap)
    return bool(int(allowed)), float(wait)


async def tenant_acquire(rds, account_id, cap: int) -> bool:
    """Per-tenant cap on concurrent in-flight deliveries. Leaked counts (crash) self-heal via TTL."""
    key = f"hr:tenant:{account_id}:active"
    n = await rds.incr(key)
    await rds.expire(key, 120)
    if n > cap:
        await rds.decr(key)
        await rds.incr(f"hr:tenant:{account_id}:throttled")
        return False
    return True


async def tenant_release(rds, account_id) -> None:
    key = f"hr:tenant:{account_id}:active"
    if await rds.decr(key) < 0:
        await rds.set(key, 0, ex=120)
