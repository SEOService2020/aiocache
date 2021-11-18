import asyncio
import itertools
import functools
import warnings

import aioredis

from aiocache.base import BaseCache
from aiocache.serializers import JsonSerializer

try:
    from aioredis.exceptions import ResponseError as IncrbyException
except ImportError:
    from aioredis.errors import ReplyError as IncrbyException

if aioredis.__version__.startswith("0."):
    AIOREDIS_MAJOR_VERSION = 0
elif aioredis.__version__.startswith("1."):
    AIOREDIS_MAJOR_VERSION = 1
else:
    AIOREDIS_MAJOR_VERSION = 2

_NOTSET = object()

if AIOREDIS_MAJOR_VERSION >= 2:
    from aioredis import Connection as _Connection, ConnectionPool as _ConnectionPool

    class Connection(_Connection):
        def __init__(self, *args, **kwargs):
            super(Connection, self).__init__(*args, **kwargs)
            self._encoding = _NOTSET

        async def read_response(self):
            """Hack to imitate an API level encoding support"""
            response = await super(Connection, self).read_response()
            response = await self.encode_decode_response(response)
            return response

        async def encode_decode_response(self, response):
            if self._encoding != _NOTSET:
                if self._encoding is None:
                    if isinstance(response, list):
                        response = [
                            value.encode("utf-8", errors="replace")
                            if isinstance(value, str)
                            else value
                            for value in response
                        ]
                    elif isinstance(response, str):
                        response = response.encode("utf-8")
                else:
                    if isinstance(response, list):
                        response = [
                            value.decode(self._encoding, errors="replace")
                            if isinstance(value, bytes)
                            else value
                            for value in response
                        ]
                    elif isinstance(response, bytes):
                        response = response.decode(self._encoding, errors="replace")
            return response

    class ConnectionPool(_ConnectionPool):
        def __init__(self, *args, **kwargs):
            super(ConnectionPool, self).__init__(*args, **kwargs)
            self.connection_class = Connection


def conn(func):
    @functools.wraps(func)
    async def wrapper(self, *args, _conn=None, **kwargs):
        if AIOREDIS_MAJOR_VERSION < 2:
            if _conn is None:
                pool = await self._get_pool()
                conn_context = await pool
                with conn_context as _conn:
                    if AIOREDIS_MAJOR_VERSION != 0:
                        _conn = aioredis.Redis(_conn)
                    return await func(self, *args, _conn=_conn, **kwargs)
            return await func(self, *args, _conn=_conn, **kwargs)
        else:
            if _conn is None:
                pool = await self._get_pool()
                try:
                    _conn = aioredis.Redis(connection_pool=pool)
                    _conn.connection = await _conn.connection_pool.get_connection("_")
                    _conn.connection._encoding = kwargs.pop("encoding", _NOTSET)
                    return await func(self, *args, _conn=_conn, **kwargs)
                finally:
                    conn = _conn.connection
                    if conn:
                        _conn.connection._encoding = _NOTSET
                        _conn.connection = None
                        await _conn.connection_pool.release(conn)
            try:
                _conn.connection._encoding = kwargs.pop("encoding", _NOTSET)
                return await func(self, *args, _conn=_conn, **kwargs)
            finally:
                conn = _conn.connection
                if conn:
                    _conn.connection._encoding = _NOTSET

    return wrapper


class RedisBackend:

    RELEASE_SCRIPT = (
        "if redis.call('get',KEYS[1]) == ARGV[1] then"
        " return redis.call('del',KEYS[1])"
        " else"
        " return 0"
        " end"
    )

    CAS_SCRIPT = (
        "if redis.call('get',KEYS[1]) == ARGV[2] then"
        "  if #ARGV == 4 then"
        "   return redis.call('set', KEYS[1], ARGV[1], ARGV[3], ARGV[4])"
        "  else"
        "   return redis.call('set', KEYS[1], ARGV[1])"
        "  end"
        " else"
        " return 0"
        " end"
    )

    pools = {}

    def __init__(
        self,
        endpoint="127.0.0.1",
        port=6379,
        db=0,
        password=None,
        pool_min_size=1,
        pool_max_size=10,
        loop=None,
        create_connection_timeout=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.port = int(port)
        self.db = int(db)
        self.password = password
        self.pool_min_size = int(pool_min_size)
        self.pool_max_size = int(pool_max_size)
        self.create_connection_timeout = (
            float(create_connection_timeout) if create_connection_timeout else None
        )
        self.__pool_lock = None
        self._loop = loop
        self._pool = None

    @property
    def _pool_lock(self):
        if self.__pool_lock is None:
            self.__pool_lock = asyncio.Lock()
        return self.__pool_lock

    async def acquire_conn(self):
        await self._get_pool()
        if AIOREDIS_MAJOR_VERSION < 2:
            conn = await self._pool.acquire()
            if AIOREDIS_MAJOR_VERSION != 0:
                conn = aioredis.Redis(conn)
        else:
            conn = aioredis.Redis(connection_pool=self._pool)
            conn.connection = await conn.connection_pool.get_connection("_")
        return conn

    async def release_conn(self, _conn):
        if AIOREDIS_MAJOR_VERSION == 0:
            self._pool.release(_conn)
        elif AIOREDIS_MAJOR_VERSION == 1:
            self._pool.release(_conn.connection)
        else:
            conn = _conn.connection
            if conn:
                _conn.connection = None
                await _conn.connection_pool.release(conn)

    @conn
    async def _get(self, key, encoding="utf-8", _conn=None):
        if AIOREDIS_MAJOR_VERSION < 2:
            return await _conn.get(key, encoding=encoding)
        else:
            return await _conn.get(key)

    @conn
    async def _gets(self, key, encoding="utf-8", _conn=None):
        return await self._get(key, encoding=encoding, _conn=_conn)

    @conn
    async def _multi_get(self, keys, encoding="utf-8", _conn=None):
        if AIOREDIS_MAJOR_VERSION < 2:
            return await _conn.mget(*keys, encoding=encoding)
        else:
            return await _conn.mget(*keys)

    @conn
    async def _set(self, key, value, ttl=None, _cas_token=None, _conn=None):
        if _cas_token is not None:
            return await self._cas(key, value, _cas_token, ttl=ttl, _conn=_conn)
        if ttl is None:
            return await _conn.set(key, value)
        if isinstance(ttl, float) and AIOREDIS_MAJOR_VERSION >= 2:
            ttl = int(ttl * 1000)
            return await _conn.psetex(key, ttl, value)
        return await _conn.setex(key, ttl, value)

    @conn
    async def _cas(self, key, value, token, ttl=None, _conn=None):
        args = [value, token]
        if ttl is not None:
            if isinstance(ttl, float):
                args += ["PX", int(ttl * 1000)]
            else:
                args += ["EX", ttl]
        if AIOREDIS_MAJOR_VERSION < 2:
            res = await self._raw("eval", self.CAS_SCRIPT, [key], args, _conn=_conn)
        else:
            args = [key] + args
            res = await self._raw("eval", self.CAS_SCRIPT, 1, *args, _conn=_conn)
        return res

    @conn
    async def _multi_set(self, pairs, ttl=None, _conn=None):
        ttl = ttl or 0

        flattened = list(itertools.chain.from_iterable((key, value) for key, value in pairs))

        if ttl:
            await self.__multi_set_ttl(_conn, flattened, ttl)
        else:
            if AIOREDIS_MAJOR_VERSION < 2:
                await _conn.mset(*flattened)
            else:
                await _conn.execute_command("MSET", *flattened)

        return True

    async def __multi_set_ttl(self, conn, flattened, ttl):
        if AIOREDIS_MAJOR_VERSION < 2:
            redis = conn.multi_exec()
            redis.mset(*flattened)
            for key in flattened[::2]:
                redis.expire(key, timeout=ttl)
            await redis.execute()
        else:
            pipeline = conn.pipeline(transaction=True)
            await pipeline.execute_command("MSET", *flattened)
            if isinstance(ttl, float):
                ttl = int(ttl * 1000)
                for key in flattened[::2]:
                    pipeline.pexpire(key, time=ttl)
            else:
                for key in flattened[::2]:
                    pipeline.expire(key, time=ttl)
            await pipeline.execute()

    @conn
    async def _add(self, key, value, ttl=None, _conn=None):
        if AIOREDIS_MAJOR_VERSION < 2:
            expx = {"expire": ttl}
            if isinstance(ttl, float):
                expx = {"pexpire": int(ttl * 1000)}
            was_set = await _conn.set(key, value, exist=_conn.SET_IF_NOT_EXIST, **expx)
        else:
            kwargs = {"nx": True}
            if isinstance(ttl, float):
                kwargs.update({"px": int(ttl * 1000)})
            else:
                kwargs.update({"ex": ttl})
            was_set = await _conn.set(key, value, **kwargs)

        if not was_set:
            raise ValueError("Key {} already exists, use .set to update the value".format(key))
        return was_set

    @conn
    async def _exists(self, key, _conn=None):
        exists = await _conn.exists(key)
        return True if exists > 0 else False

    @conn
    async def _increment(self, key, delta, _conn=None):
        try:
            return await _conn.incrby(key, delta)
        except IncrbyException:
            raise TypeError("Value is not an integer") from None

    @conn
    async def _expire(self, key, ttl, _conn=None):
        if ttl == 0:
            return await _conn.persist(key)
        return await _conn.expire(key, ttl)

    @conn
    async def _delete(self, key, _conn=None):
        return await _conn.delete(key)

    @conn
    async def _clear(self, namespace=None, _conn=None):
        if namespace:
            keys = await _conn.keys("{}:*".format(namespace))
            if keys:
                await _conn.delete(*keys)
        else:
            await _conn.flushdb()
        return True

    @conn
    async def _raw(self, command, *args, encoding="utf-8", _conn=None, **kwargs):
        if command in ["get", "mget"] and AIOREDIS_MAJOR_VERSION < 2:
            kwargs["encoding"] = encoding
        return await getattr(_conn, command)(*args, **kwargs)

    async def _redlock_release(self, key, value):
        if AIOREDIS_MAJOR_VERSION < 2:
            return await self._raw("eval", self.RELEASE_SCRIPT, [key], [value])
        else:
            return await self._raw("eval", self.RELEASE_SCRIPT, 1, key, value)

    async def _close(self, *args, **kwargs):
        if self._pool is not None:
            if AIOREDIS_MAJOR_VERSION < 2:
                await self._pool.clear()
            else:
                await self._pool.disconnect(inuse_connections=True)
                self._pool.reset()

    async def _get_pool(self):
        async with self._pool_lock:
            if self._pool is None:
                if AIOREDIS_MAJOR_VERSION < 2:
                    kwargs = {
                        "db": self.db,
                        "password": self.password,
                        "loop": self._loop,
                        "encoding": "utf-8",
                        "minsize": self.pool_min_size,
                        "maxsize": self.pool_max_size,
                    }
                    if AIOREDIS_MAJOR_VERSION == 1:
                        kwargs["create_connection_timeout"] = self.create_connection_timeout

                    self._pool = await aioredis.create_pool((self.endpoint, self.port), **kwargs)
                else:
                    if self._loop is not None:
                        warnings.warn(
                            "Parameter 'loop' has been obsolete since aioredis 2.0.0.",
                            DeprecationWarning,
                        )
                    if self.pool_min_size != 1:
                        warnings.warn(
                            "Parameter 'pool_min_size' has been obsolete since aioredis 2.0.0.",
                            DeprecationWarning,
                        )
                    kwargs = {
                        "max_connections": self.pool_max_size,
                        "host": self.endpoint,
                        "port": self.port,
                        "db": self.db,
                        "password": self.password,
                        "encoding": "utf-8",
                        "decode_responses": True,
                        "socket_connect_timeout": self.create_connection_timeout,
                    }
                    self._pool = ConnectionPool(**kwargs)

            return self._pool


class RedisCache(RedisBackend, BaseCache):
    """
    Redis cache implementation with the following components as defaults:
        - serializer: :class:`aiocache.serializers.JsonSerializer`
        - plugins: []

    Config options are:

    :param serializer: obj derived from :class:`aiocache.serializers.BaseSerializer`.
    :param plugins: list of :class:`aiocache.plugins.BasePlugin` derived classes.
    :param namespace: string to use as default prefix for the key used in all operations of
        the backend. Default is None.
    :param timeout: int or float in seconds specifying maximum timeout for the operations to last.
        By default its 5.
    :param endpoint: str with the endpoint to connect to. Default is "127.0.0.1".
    :param port: int with the port to connect to. Default is 6379.
    :param db: int indicating database to use. Default is 0.
    :param password: str indicating password to use. Default is None.
    :param pool_min_size: int minimum pool size for the redis connections pool. Default is 1
    :param pool_max_size: int maximum pool size for the redis connections pool. Default is 10
    :param create_connection_timeout: int timeout for the creation of connection,
        only for aioredis>=1. Default is None
    """

    NAME = "redis"

    def __init__(self, serializer=None, **kwargs):
        super().__init__(**kwargs)
        self.serializer = serializer or JsonSerializer()

    @classmethod
    def parse_uri_path(self, path):
        """
        Given a uri path, return the Redis specific configuration
        options in that path string according to iana definition
        http://www.iana.org/assignments/uri-schemes/prov/redis

        :param path: string containing the path. Example: "/0"
        :return: mapping containing the options. Example: {"db": "0"}
        """
        options = {}
        db, *_ = path[1:].split("/")
        if db:
            options["db"] = db
        return options

    def _build_key(self, key, namespace=None):
        if namespace is not None:
            return "{}{}{}".format(namespace, ":" if namespace else "", key)
        if self.namespace is not None:
            return "{}{}{}".format(self.namespace, ":" if self.namespace else "", key)
        return key

    def __repr__(self):  # pragma: no cover
        return "RedisCache ({}:{})".format(self.endpoint, self.port)
