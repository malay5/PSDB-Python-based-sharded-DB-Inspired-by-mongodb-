import uuid


class DistributedLock:
    def __init__(self, redis_client, lock_key='raft_lock'):
        self.redis = redis_client
        self.lock_key = lock_key
        self.lock_value = str(uuid.uuid4())

    def acquire(self, timeout=5):
        return self.redis.set(self.lock_key, self.lock_value, nx=True, ex=timeout)

    def release(self):
        lock_value = self.redis.get(self.lock_key)
        if lock_value and lock_value.decode() == self.lock_value:
            self.redis.delete(self.lock_key)
