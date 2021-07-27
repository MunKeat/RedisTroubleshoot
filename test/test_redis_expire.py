# -*- coding: utf-8 -*-
import copy
import time
import unittest

from src.redis_common import RedisCommon
from src.redis_expire import RedisExpire

class TestRedisExpire(unittest.TestCase):
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
                     "ttl": 1,
                     "print_keys": True})
        redis_verify_functionality = RedisExpire(**args)
        result = redis_verify_functionality.set_ttl_single()
        time.sleep(1)
        expected_result = {"keys_deleted": 1,
                           "pattern": pattern_to_be_tested,
                           "no_ttl_only": False,
                           "keys": ["HEAT"]}
        assert result["keys_deleted"] == expected_result["keys_deleted"], f"Number of keys deleted differ. {result['keys_deleted']} key(s) expected, deleted {result['keys_deleted']} key(s) instead."
        assert result["keys"] == expected_result["keys"], f"Keys encountered differ. {expected_result['keys']} key(s) expected, found {result['keys']} key(s) instead."

    def test_single_nonexistent_key(self):
        pattern_to_be_tested = "CHAFF"
        args = copy.deepcopy(self.COMMON_ARG)
        args.update({"pattern": pattern_to_be_tested,
                     "no_ttl_only": False,
                     "ttl": 1,
                     "print_keys": True})
        redis_verify_functionality = RedisExpire(**args)
        result = redis_verify_functionality.set_ttl_single()
        expected_result = {"keys_deleted": 0,
                           "pattern": pattern_to_be_tested,
                           "no_ttl_only": False,
                           "keys": []}
        assert result["keys_deleted"] == expected_result["keys_deleted"], f"Number of keys deleted differ. {result['keys_deleted']} key(s) expected, deleted {result['keys_deleted']} key(s) instead."
        assert result["keys"] == expected_result["keys"], f"Keys encountered differ. {expected_result['keys']} key(s) expected, found {result['keys']} key(s) instead."

    def test_pattern_existing_keys(self):
        pattern_to_be_tested = "*EAT"
        time.sleep(1)
        args = copy.deepcopy(self.COMMON_ARG)
        args.update({"pattern": pattern_to_be_tested,
                     "no_ttl_only": False,
                     "ttl": 1,
                     "print_keys": True})
        redis_verify_functionality = RedisExpire(**args)
        result = redis_verify_functionality.set_ttl_pattern()
        time.sleep(1)
        result["keys"] = sorted([key.decode("utf-8") for key in result["keys"]])
        expected_result = {"keys_deleted": 3,
                           "pattern": pattern_to_be_tested,
                           "no_ttl_only": False,
                           "keys": sorted(["EAT", "HEAT", "WHEAT"])}
        assert result["keys_deleted"] == expected_result["keys_deleted"], f"Number of keys deleted differ. {result['keys_deleted']} key(s) expected, deleted {result['keys_deleted']} key(s) instead."
        assert result["keys"] == expected_result["keys"], f"Keys encountered differ. {expected_result['keys']} key(s) expected, found {result['keys']} key(s) instead."


    def tearDown(self):
        self.redis_client.flushall()
        assert self.redis_client.dbsize() == 0, f"The Redis instance is expected to be empty, however, dbsize returns {self.redis_client.dbsize()}"
