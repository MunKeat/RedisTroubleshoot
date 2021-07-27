# -*- coding: utf-8 -*-
import copy
import time
import unittest

from src.redis_common import RedisCommon
from src.redis_flushall import RedisFlushall


class TestRedisFlushall(unittest.TestCase):
    COMMON_ARG = {"host": "127.0.0.1",
                  "port": "6379",
                  "cluster_mode": False,
                  "authentication": None,
                  "decode_responses": True}

    def setUp(self):
        self.redis_client = RedisCommon(**self.COMMON_ARG).get_redis_client()
        self.redis_client.set("WHEAT", 5)
        self.redis_client.set("HEAT", 4)
        self.redis_client.set("EAT", 3)
        self.redis_client.set("AT", 2)
        self.redis_client.set("A", 1)

    def test_flushall(self):
        args = copy.deepcopy(self.COMMON_ARG)
        args.update({"flushall": True,
                     "asynchronous": False})
        redis_flushall_functionality = RedisFlushall(**args)
        redis_flushall_functionality.execute()
        assert self.redis_client.dbsize() == 0, f"The Redis instance is expected to be empty, however, dbsize returns {self.redis_client.dbsize()}"

    def tearDown(self):
        self.redis_client.flushall()
        assert self.redis_client.dbsize() == 0, f"The Redis instance is expected to be empty, however, dbsize returns {self.redis_client.dbsize()}"
