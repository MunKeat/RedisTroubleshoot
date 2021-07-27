# -*- coding: utf-8 -*-
import time

from argparse import ArgumentParser
from pprint import pprint
from src.redis_common import RedisCommon


class RedisVerify(RedisCommon):
    NO_TTL_SET = -1
    DEFAULT_SLEEP = 1
    DEFAULT_SLEEP_BATCH = 1000
    DEFAULT_COUNT = 1000

    WILDCARD = "*"

    def __init__(self, host, port, cluster_mode, authentication, decode_responses, pattern, no_ttl_only, print_keys):
        super().__init__(host, port, cluster_mode, authentication, decode_responses)
        self.redis_client = self.get_redis_client()
        self.redis_pipeline = self.get_redis_pipeline()
        # Parameters for verification
        self.pattern = pattern
        self.no_ttl_only = no_ttl_only
        self.print_keys = print_keys

    def verify_pattern_naive(self):
        sleep_counter = 0
        cumulative_keys = []
        start_time = time.time()
        # Begin iterating through Redis
        for scan_counter, key_value in enumerate(self.redis_client.scan_iter(match=self.pattern, count=self.DEFAULT_COUNT)):
            if (scan_counter > 0) and (self.DEFAULT_SLEEP > 0) and (scan_counter % self.DEFAULT_SLEEP_BATCH == 0):
                time.sleep(self.DEFAULT_SLEEP)
                sleep_counter += 1
            cumulative_keys.append(key_value)
        # End statistic
        end_time = time.time()
        # Calculate statistics
        total_time = end_time - start_time
        sleep_time = self.DEFAULT_SLEEP * sleep_counter
        execution_time = total_time - sleep_time
        result = {"total_time": total_time,
                  "sleep_time": sleep_time,
                  "execution_time": execution_time,
                  "keys_encountered": len(cumulative_keys),
                  "pattern": self.pattern,
                  "no_ttl_only": self.no_ttl_only}
        if self.print_keys:
            result["keys"] = cumulative_keys
        return result

    def verify_pattern_without_ttl(self):
        sleep_counter = 0
        cumulative_keys = []
        temp_cumulative_keys = []
        start_time = time.time()
        # Begin iterating through Redis
        for scan_counter, key_value in enumerate(self.redis_client.scan_iter(match=self.pattern, count=self.DEFAULT_COUNT)):
            self.redis_pipeline.ttl(key_value)
            temp_cumulative_keys.append(key_value)
            if (scan_counter > 0) and (self.DEFAULT_SLEEP > 0) and (scan_counter % self.DEFAULT_SLEEP_BATCH == 0):
                time.sleep(self.DEFAULT_SLEEP)
                sleep_counter += 1
                # Execute
                resp = self.redis_pipeline.execute()
                cumulative_keys.extend([key for key, ttl in zip(temp_cumulative_keys, resp) if ttl == self.NO_TTL_SET])
                temp_cumulative_keys = []
        # Final loop
        resp = self.redis_pipeline.execute()
        cumulative_keys.extend([key for key, ttl in zip(temp_cumulative_keys, resp) if ttl == self.NO_TTL_SET])
        # End statistic
        end_time = time.time()
        # Calculate statistics
        total_time = end_time - start_time
        sleep_time = self.DEFAULT_SLEEP * sleep_counter
        execution_time = total_time - sleep_time
        result = {"total_time": total_time,
                  "sleep_time": sleep_time,
                  "execution_time": execution_time,
                  "keys_encountered": len(cumulative_keys),
                  "pattern": self.pattern,
                  "no_ttl_only": self.no_ttl_only}
        if self.print_keys:
            result["keys"] = cumulative_keys
        return result

    def verify_single(self):
        cumulative_keys = []
        start_time = time.time()
        keys_encountered = int(self.redis_client.exists(self.pattern))
        cumulative_keys.append(self.pattern)
        if self.no_ttl_only and self.redis_client.ttl(self.pattern) != self.NO_TTL_SET:
            keys_encountered = 0
            cumulative_keys = []
        elif keys_encountered == 0:
            cumulative_keys = []
        end_time = time.time()
        # Calculate statistics
        total_time = end_time - start_time
        result = {"total_time": total_time,
                  "sleep_time": 0,
                  "execution_time": total_time,
                  "keys_encountered": keys_encountered,
                  "pattern": self.pattern,
                  "no_ttl_only": self.no_ttl_only}
        if self.print_keys:
            result["keys"] = cumulative_keys
        return result

    def execute(self):
        result = {}
        if (self.WILDCARD not in self.pattern):
            result = self.verify_single()
        elif (self.WILDCARD in self.pattern) and (not self.no_ttl_only):
            result = self.verify_pattern_naive()
        elif (self.WILDCARD in self.pattern) and (self.no_ttl_only):
            result = self.verify_pattern_without_ttl()
        return result


def get_arguments():
    parser = ArgumentParser(description="Check keys available in the Redis instance")
    parser.add_argument("--host", type=str, required=True, help="Redis instance / cluster endpoint")
    parser.add_argument("--port", type=str, required=False, help="Redis instance / cluster port")
    # Check if cluster mode or not
    cluster_mode_group = parser.add_mutually_exclusive_group(required=True)
    cluster_mode_group.add_argument("--cluster_mode_enabled",
                                    action="store_true",
                                    dest="cluster_mode",
                                    help="Indicate that Redis instance is cluster mode enabled")
    cluster_mode_group.add_argument("--cluster_mode_disabled",
                                    action="store_false",
                                    dest="cluster_mode",
                                    help="Indicate that Redis instance is cluster mode disabled")
    parser.add_argument("--authentication", type=str, required=False, help="Authentication required for Redis")
    parser.add_argument("--pattern", type=str, required=True, help="Pattern to match desired key(s)")
    parser.add_argument("--no_ttl_only", action="store_true", required=False, help="Check keys without TTL")
    parser.add_argument("--print_keys", action="store_true", required=False, help="Print key(s)")
    return parser.parse_args()


def main():
    args = get_arguments()
    task = RedisVerify(**vars(args))
    result = task.execute()
    pprint(result)


if __name__ == "__main__":
    main()
