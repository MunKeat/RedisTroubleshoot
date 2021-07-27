# -*- coding: utf-8 -*-
import time

from argparse import ArgumentParser
from pprint import pprint
from src.redis_common import RedisCommon

DEFAULT_REDIS_PORT = 6379


class RedisExpire(RedisCommon):
    NO_TTL_SET = -1
    DEFAULT_SLEEP = 1
    DEFAULT_SLEEP_BATCH = 1000
    DEFAULT_COUNT = 1000

    WILDCARD = "*"

    def __init__(self, host, port, cluster_mode, authentication, decode_responses, pattern, no_ttl_only, ttl, print_keys):
        super().__init__(host, port, cluster_mode, authentication, decode_responses)
        self.redis_client = self.get_redis_client()
        self.redis_pipeline = self.get_redis_pipeline()
        # Argument for expiring keys
        self.pattern = pattern
        self.no_ttl_only = no_ttl_only
        self.ttl = ttl
        self.print_keys = print_keys

    def set_ttl_pattern(self):
        # Set variables for deletion
        keys_deleted = 0
        sleep_counter = 0
        cumulative_keys = []
        start_time = time.time()
        for key_value in self.redis_client.scan_iter(match=self.pattern, count=self.DEFAULT_COUNT):
            if (keys_deleted > 0 and
                    self.DEFAULT_SLEEP > 0 and
                    keys_deleted % self.DEFAULT_SLEEP_BATCH == 0):
                time.sleep(self.DEFAULT_SLEEP)
                self.redis_pipeline.execute()
                sleep_counter += 1
            # Begin deletion for non-TTL set
            if self.no_ttl_only and self.redis_client.ttl(key_value) == self.NO_TTL_SET:
                keys_deleted += 1
                self.redis_pipeline.expire(key_value, self.ttl)
                cumulative_keys.append(key_value)
                continue
            keys_deleted += 1
            self.redis_pipeline.expire(key_value, self.ttl)
            cumulative_keys.append(key_value)
        self.redis_pipeline.execute()
        end_time = time.time()
        # Calculate statistics
        total_time = end_time - start_time
        sleep_time = self.DEFAULT_SLEEP * sleep_counter
        execution_time = total_time - sleep_time
        result = {"total_time": total_time,
                  "sleep_time": sleep_time,
                  "execution_time": execution_time,
                  "keys_deleted": keys_deleted,
                  "pattern": self.pattern,
                  "ttl": self.ttl,
                  "no_ttl_only": self.no_ttl_only}
        if self.print_keys:
            result["keys"] = cumulative_keys
        return result

    def set_ttl_single(self):
        # Set variables for deletion
        keys_deleted = 0
        start_time = time.time()
        cumulative_key_deleted = []
        # Check if key exists
        if self.redis_client.exists(self.pattern):
            # Begin deletion for non-TTL set
            if self.no_ttl_only and self.redis_client.ttl(self.pattern) != self.NO_TTL_SET:
                end_time = time.time()
                total_time = end_time - start_time
                result = {"total_time": total_time,
                          "sleep_time": 0,
                          "execution_time": total_time,
                          "keys_deleted": keys_deleted,
                          "pattern": self.pattern,
                          "ttl": self.ttl,
                          "no_ttl_only": self.no_ttl_only}
                if self.print_keys:
                    result["keys"] = cumulative_key_deleted
                return result
            keys_deleted += 1
            self.redis_client.expire(self.pattern, self.ttl)
            cumulative_key_deleted.append(self.pattern)
        end_time = time.time()
        # Calculate statistics
        total_time = end_time - start_time
        result = {"total_time": total_time,
                  "sleep_time": 0,
                  "execution_time": total_time,
                  "keys_deleted": keys_deleted,
                  "pattern": self.pattern,
                  "ttl": self.ttl,
                  "no_ttl_only": self.no_ttl_only}
        if self.print_keys:
            result["keys"] = cumulative_key_deleted
        return result

    def execute(self):
        if self.WILDCARD in self.pattern:
            return self.set_ttl_pattern()
        else:
            return self.set_ttl_single()


def get_arguments():
    parser = ArgumentParser(description="Script to expire / delete keys in Redis instances")
    parser.add_argument("--host", type=str, required=True, help="Redis Host")
    parser.add_argument("--port", type=int, default=DEFAULT_REDIS_PORT, help="Redis Port - Defaults to {}".format(str(DEFAULT_REDIS_PORT)))
    parser.add_argument("--authentication", type=str, help="Authentication required for Redis")
    parser.add_argument("--pattern", type=str, required=True, help="Pattern of keys to expire")
    parser.add_argument("--no_ttl_only", action="store_true", help="Expire only keys without TTL")
    parser.add_argument("--ttl", type=int, required=True, help="Set TTL")
    parser.add_argument("--print_keys", action="store_true", required=False, help="Print key(s)")
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
    parser.add_argument("--decode_responses", type=bool, default=False, help="Authentication required for Redis")
    return parser.parse_args()


def main():
    args = get_arguments()
    task = RedisExpire(**vars(args))
    result = task.execute()
    pprint(result)


if __name__ == "__main__":
    main()
