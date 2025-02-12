import redis
from datetime import datetime

#  connection with Redis server
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Flush the Redis database
r.flushdb()
print(f"Redis flushed at {datetime.now()}")
