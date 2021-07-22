# -*- coding: utf-8 -*-
import copy
import unittest

from src.redis_common import RedisCommon
from src.redis_verify import RedisVerify


class TestRedisVerify(unittest.TestCase):
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

    def test_single_existing_key(self):
        pattern_to_be_tested = "HEAT"
        args = copy.deepcopy(self.COMMON_ARG)
        args.update({"pattern": pattern_to_be_tested,
                     "no_ttl_only": False,
                     "print_keys": True})
        redis_verify_functionality = RedisVerify(**args)
        result = redis_verify_functionality.verify_single()
        expected_result = { "keys_encountered": 1,
                            "pattern": pattern_to_be_tested,
                            "no_ttl_only": False,
                            "print_keys": [pattern_to_be_tested]}
        assert result["keys_encountered"] == expected_result["keys_encountered"], f"Number of keys encountered differ. 1 key expected, found {result['keys_encountered']} key(s) instead."

    def test_single_nonexistent_key(self):
        pattern_to_be_tested = "CHAFF"
        args = copy.deepcopy(self.COMMON_ARG)
        args.update({"pattern": pattern_to_be_tested,
                     "no_ttl_only": False,
                     "print_keys": True})
        redis_verify_functionality = RedisVerify(**args)
        result = redis_verify_functionality.verify_single()
        expected_result = { "keys_encountered": 0,
                            "pattern": pattern_to_be_tested,
                            "no_ttl_only": False,
                            "print_keys": []}
        assert result["keys_encountered"] == expected_result["keys_encountered"], f"Number of keys encountered differ. 0 key expected, found {result['keys_encountered']} key(s) instead."

    def tearDown(self):
        self.redis_client.flushall()
