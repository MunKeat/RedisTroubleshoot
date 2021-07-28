# -*- coding: utf-8 -*-
import copy
import unittest

from src.redis_info import RedisInfo


class TestRedisInfo(unittest.TestCase):
    COMMON_ARG = {"host": "127.0.0.1",
                  "port": "6379",
                  "authentication": None,
                  "decode_responses": True}

    def test_processed_nonclustered_redis_log(self):
        args = copy.deepcopy(self.COMMON_ARG)
        args.update({"cluster_mode": False,
                     "filter_keys": "key|ratio"})
        redis_info_functionality = RedisInfo(**args)
        sample_info_log = {"active_defrag_hits": 0,
                           "active_defrag_key_hits": 0,
                           "active_defrag_key_misses": 0,
                           "active_defrag_misses": 0,
                           "active_defrag_running": 0,
                           "allocator_active": 100000000,
                           "allocator_allocated": 100000000,
                           "allocator_frag_bytes": 500000,
                           "allocator_frag_ratio": 1.0,
                           "allocator_resident": 100000000,
                           "allocator_rss_bytes": 500000,
                           "allocator_rss_ratio": 1.00}
        output = redis_info_functionality.filter_info_log(sample_info_log)
        expected_output = {"active_defrag_key_hits": 0, 
                           "active_defrag_key_misses": 0, 
                           "allocator_frag_ratio": 1.0, 
                           "allocator_rss_ratio": 1.0}
        assert output == expected_output, f"The output {expected_output} is expected, however, output {output} was found"

    def test_processed_clustered_redis_log(self):
        args = copy.deepcopy(self.COMMON_ARG)
        args.update({"cluster_mode": False,
                     "filter_keys": "active"})
        redis_info_functionality = RedisInfo(**args)
        # TODO: Current workaround as clustered dockerised Redis setup is not done yet
        redis_info_functionality.cluster_mode=True
        # TODO: END
        sample_info_log = {"10.10.0.0": {"active_defrag_hits": 0,
                                         "active_defrag_key_hits": 0,
                                         "active_defrag_key_misses": 0,
                                         "active_defrag_misses": 0,
                                         "active_defrag_running": 0,
                                         "allocator_active": 100000000,
                                         "allocator_allocated": 100000000,
                                         "allocator_frag_bytes": 500000,
                                         "allocator_frag_ratio": 1.0,
                                         "allocator_resident": 100000000,
                                         "allocator_rss_bytes": 500000,
                                         "allocator_rss_ratio": 1.00},
                           "10.10.0.1": {"active_defrag_hits": 1,
                                         "active_defrag_key_hits": 1,
                                         "active_defrag_key_misses": 1,
                                         "active_defrag_misses": 1,
                                         "active_defrag_running": 1,
                                         "allocator_active": 100000000,
                                         "allocator_allocated": 100000000,
                                         "allocator_frag_bytes": 500000,
                                         "allocator_frag_ratio": 1.0,
                                         "allocator_resident": 100000000,
                                         "allocator_rss_bytes": 500000,
                                         "allocator_rss_ratio": 1.00}}
        output = redis_info_functionality.filter_info_log(sample_info_log)
        expected_output = {"10.10.0.0": {"active_defrag_hits": 0,
                                         "active_defrag_key_hits": 0,
                                         "active_defrag_key_misses": 0,
                                         "active_defrag_misses": 0,
                                         "active_defrag_running": 0,
                                         "allocator_active": 100000000},
                           "10.10.0.1": {"active_defrag_hits": 1,
                                         "active_defrag_key_hits": 1,
                                         "active_defrag_key_misses": 1,
                                         "active_defrag_misses": 1,
                                         "active_defrag_running": 1,
                                         "allocator_active": 100000000}}
        assert output == expected_output, f"The output {expected_output} is expected, however, output {output} was found"

    def test_invalid_filter_keys(self):
        # Test Clustered
        clustered_sample_info_log = {"10.10.0.0": {"active_defrag_hits": 0,
                                                   "active_defrag_key_hits": 0,
                                                   "active_defrag_key_misses": 0,
                                                   "active_defrag_misses": 0,
                                                   "active_defrag_running": 0,
                                                   "allocator_active": 100000000,
                                                   "allocator_allocated": 100000000,
                                                   "allocator_frag_bytes": 500000,
                                                   "allocator_frag_ratio": 1.0,
                                                   "allocator_resident": 100000000,
                                                   "allocator_rss_bytes": 500000,
                                                   "allocator_rss_ratio": 1.00},
                                     "10.10.0.1": {"active_defrag_hits": 1,
                                                   "active_defrag_key_hits": 1,
                                                   "active_defrag_key_misses": 1,
                                                   "active_defrag_misses": 1,
                                                   "active_defrag_running": 1,
                                                   "allocator_active": 100000000,
                                                   "allocator_allocated": 100000000,
                                                   "allocator_frag_bytes": 500000,
                                                   "allocator_frag_ratio": 1.0,
                                                   "allocator_resident": 100000000,
                                                   "allocator_rss_bytes": 500000,
                                                   "allocator_rss_ratio": 1.00}}
        nonclustered_sample_info_log = {"active_defrag_hits": 0,
                                        "active_defrag_key_hits": 0,
                                        "active_defrag_key_misses": 0,
                                        "active_defrag_misses": 0,
                                        "active_defrag_running": 0,
                                        "allocator_active": 100000000,
                                        "allocator_allocated": 100000000,
                                        "allocator_frag_bytes": 500000,
                                        "allocator_frag_ratio": 1.0,
                                        "allocator_resident": 100000000,
                                        "allocator_rss_bytes": 500000,
                                        "allocator_rss_ratio": 1.00}
        invalid_filter_key = "invalid|nonsense"
        # Begin test
        args = copy.deepcopy(self.COMMON_ARG)
        args.update({"cluster_mode": False,
                     "filter_keys": invalid_filter_key})
        redis_info_functionality = RedisInfo(**args)
        nonclustered_output = redis_info_functionality.filter_info_log(nonclustered_sample_info_log)
        assert nonclustered_output == nonclustered_sample_info_log, f"The output {nonclustered_sample_info_log} is expected, however, output {nonclustered_output} was found"
        # TODO: Current workaround as clustered dockerised Redis setup is not done yet
        redis_info_functionality.cluster_mode=True
        # TODO: END
        clustered_output = redis_info_functionality.filter_info_log(clustered_sample_info_log)
        assert clustered_output == clustered_sample_info_log, f"The output {clustered_sample_info_log} is expected, however, output {clustered_output} was found"
