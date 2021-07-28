# -*- coding: utf-8 -*-
import re

from argparse import ArgumentParser
from pprint import pprint
from redis_common import RedisCommon

DEFAULT_REDIS_PORT = 6379


class RedisInfo(RedisCommon):
    def __init__(self, host, port, cluster_mode, authentication, decode_responses, filter_keys=None):
        super().__init__(host, port, cluster_mode, authentication, decode_responses)
        self.cluster_mode = cluster_mode
        self.redis_client = self.get_redis_client()
        self.filter_keys = re.compile(filter_keys) if filter_keys is not None else None

    def get_info_log(self):
        return self.redis_client.log()

    def filter_info_log(self, log):
        processed_log = {}
        if self.filter_keys is None:
            processed_log = log
        elif self.filter_keys and self.cluster_mode:
            for redis_node, redis_info in log.items():
                temporary_redis_info = {key: value for key, value in redis_info.items()
                                        if self.filter_keys.search(key)}
                processed_log[redis_node] = temporary_redis_info if temporary_redis_info != {} else redis_info
        elif self.filter_keys and not self.cluster_mode:
            temporary_redis_info = {key: value for key, value in log.items()
                                    if self.filter_keys.search(key)}
            processed_log = temporary_redis_info if temporary_redis_info != {} else log
        return processed_log

    def execute(self):
        # Get Redis info log
        logs = self.get_info_log()
        processed_log = self.filter_info_log(logs)
        return processed_log

def get_arguments():
    parser = ArgumentParser(description="Obtain INFO log from Redis instances")
    parser.add_argument("--host", type=str, required=True, help="Redis Host")
    parser.add_argument("--port", type=int, default=DEFAULT_REDIS_PORT, help="Redis Port - Defaults to {}".format(str(DEFAULT_REDIS_PORT)))
    parser.add_argument("--authentication", type=str, help="Authentication required for Redis")
    # Check if cluster mode or not
    cluster_mode_group = parser.add_mutually_exclusive_group(required=True)
    cluster_mode_group.add_argument("--cluster_mode_enabled",
                                    action="store_true",
                                    dest="cluster_mode",
                                    help="Indicate Redis instance is cluster mode enabled")
    cluster_mode_group.add_argument("--cluster_mode_disabled",
                                    action="store_false",
                                    dest="cluster_mode",
                                    help="Indicate Redis instance is cluster mode disabled")
    parser.add_argument("--decode_responses", type=bool, default=False, help="Authentication required for Redis")
    parser.add_argument("--filter_keys", type=str, default=False, help="Regex expression for desired keys")

    return parser.parse_args()

def main():
    args = get_arguments()
    task = RedisInfo(**vars(args))
    result = task.execute()
    pprint(result)

if __name__ == "__main__":
    main()

