import json
import logging
from typing import Any, Callable, TypeVar

import redis
from redis.sentinel import Sentinel
from google.protobuf.json_format import MessageToDict, ParseDict

from config import REDIS_SENTINEL_HOST, REDIS_SENTINEL_PORT, REDIS_MASTER_SET

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

sentinel = Sentinel([(REDIS_SENTINEL_HOST, REDIS_SENTINEL_PORT)], socket_timeout=0.1)
redis_client = sentinel.master_for(REDIS_MASTER_SET, socket_timeout=0.1, decode_responses=True)

T = TypeVar("T")

def get_cached(key: str) -> str | None:
    try:
        value = redis_client.get(key)
        if value:
            logger.info(f"Cache HIT: {key}")
            return value
        logger.info(f"Cache MISS: {key}")
        return None
    except redis.RedisError as e:
        logger.error(f"Redis error get: {e}")
        return None

def set_cached(key: str, value: str, ttl: int = 300) -> None:
    try:
        redis_client.setex(key, ttl, value)
    except redis.RedisError as e:
        logger.error(f"Redis error set: {e}")

def invalidate_cache(pattern: str) -> None:
    try:
        keys = redis_client.keys(pattern)
        if keys:
            redis_client.delete(*keys)
            logger.info(f"Invalidated cache for pattern: {pattern}, keys: {len(keys)}")
    except redis.RedisError as e:
        logger.error(f"Redis error invalidate: {e}")

def cache_proto(key_func: Callable[..., str], ttl: int = 300, proto_class: Any = None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            key = key_func(*args, **kwargs)
            cached_json = get_cached(key)
            if cached_json:
                try:
                    message = proto_class()
                    ParseDict(json.loads(cached_json), message)
                    return message
                except Exception as e:
                    logger.error(f"Error parsing cached proto: {e}")

            result = func(*args, **kwargs)
            
            try:
                json_str = json.dumps(MessageToDict(result))
                set_cached(key, json_str, ttl)
            except Exception as e:
                logger.error(f"Error caching proto: {e}")
            
            return result
        return wrapper
    return decorator