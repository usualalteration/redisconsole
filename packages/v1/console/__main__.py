#--kind python:default
#--web true
# Note: this timeout is 5 minutes - 10 minutes is max allowed
#--timeout 300000
import types, os, console

builder = []
## build-context ##
#--param ext_redisurl "$ext_redisurl"
#--param ext_redis_prefix "$ext_redis_prefix"
#--param REDIS_URL "$REDIS_URL"
#--param REDIS_PREFIX "$REDIS_PREFIX"
import redis
def init_redis(args, ctx):
  # console.resolve_redis_config prefers ext_redisurl/ext_redis_prefix and
  # falls back to REDIS_URL/REDIS_PREFIX; the module validates missing/empty.
  url, prefix = console.resolve_redis_config(args, os.environ)
  ctx.REDIS_PREFIX = prefix
  ctx.REDIS = redis.from_url(url, decode_responses=True) if url else None
builder.append(init_redis)

def main(args):
  try:
    ctx = types.SimpleNamespace()
    for fn in builder: fn(args, ctx)
    return { "body": console.main(args, ctx=ctx) }
  except Exception as e:
    import traceback
    traceback.print_exc()
    return {
      "body": {"error": str(e) },
      "statusCode": 500
    }
