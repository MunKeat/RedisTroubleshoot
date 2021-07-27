# -*- coding: utf-8 -*-
import copy
import unittest

from src.redis_slowlog import RedisSlowlog


class TestRedisSlowlog(unittest.TestCase):
    COMMON_ARG = {"host": "127.0.0.1",
                  "port": "6379",
                  "cluster_mode": False,
                  "authentication": None,
                  "decode_responses": False}

    def test_cluster_mode_processing(self):
        args = copy.deepcopy(self.COMMON_ARG)
        redis_slowlog_functionality = RedisSlowlog(**args)
        raw_clustered_redis_output = {"10.10.1.1:6379": [],
                                      "10.10.1.2:6379": [{"command": b"HMGET state:12345 state lock_expire_a",
                                                          "duration": 61265,
                                                          "id": 4,
                                                          "start_time": 1627346702},
                                                         {"command": b"HMGET state:23456 state lock_expire_a",
                                                          "duration": 10973,
                                                          "id": 3,
                                                          "start_time": 1627254663}],
                                      "10.10.1.3:6379": [{"command": b"HMGET state:34567 state lock_expire_a",
                                                          "duration": 39980,
                                                          "id": 2,
                                                          "start_time": 1627353330},
                                                         {"command": b"HMGET state:45678 state lock_expire_a",
                                                          "duration": 39416,
                                                          "id": 1,
                                                          "start_time": 1627266629}]}
        expected_output = [{"command": b"HMGET state:12345 state lock_expire_a",
                            "duration": 61265,
                            "host": "10.10.1.2:6379",
                            "id": 4,
                            "start_time": 1627346702},
                           {"command": b"HMGET state:23456 state lock_expire_a",
                            "duration": 10973,
                            "host": "10.10.1.2:6379",
                            "id": 3,
                            "start_time": 1627254663},
                           {"command": b"HMGET state:34567 state lock_expire_a",
                            "duration": 39980,
                            "host": "10.10.1.3:6379",
                            "id": 2,
                            "start_time": 1627353330},
                           {"command": b"HMGET state:45678 state lock_expire_a",
                            "duration": 39416,
                            "host": "10.10.1.3:6379",
                            "id": 1,
                            "start_time": 1627266629}]
        output = redis_slowlog_functionality.cluster_mode_processing(raw_clustered_redis_output)
        # Sorting both output to ensure equality
        expected_output = sorted(expected_output, key=lambda x: x.get("start_time", ""), reverse=True)
        output = sorted(output, key=lambda x: x.get("start_time", ""), reverse=True)
        for expected_entry, entry in zip(expected_output, output):
            assert expected_entry == entry, f"Output differ. Expected {expected_output}, found {output} instead."

    def test_append_log(self):
        args = copy.deepcopy(self.COMMON_ARG)
        redis_slowlog_functionality = RedisSlowlog(**args)
        output = redis_slowlog_functionality.append_logs([{"command": b"HMGET state:45678 state lock_expire_a",
                                                           "duration": 39416,
                                                           "host": "10.10.1.3:6379",
                                                           "id": 1,
                                                           "start_time": 1627266629}])
        expected_output = [ {"command": b"HMGET state:45678 state lock_expire_a",
                             "duration_in_microsec": 39416,
                             "duration_in_millisec": 39,
                             "host": "10.10.1.3:6379",
                             "id": 1,
                             "readable_start_time": "2021-07-26 02:30:29+0000",
                             "start_time": 1627266629}]
        assert expected_output == output, f"Output differ. Expected {expected_output}, found {output} instead."
