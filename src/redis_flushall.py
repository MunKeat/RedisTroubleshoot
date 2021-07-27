# -*- coding: utf-8 -*-
"""Useful for Redis cluster with multiple master nodes
   as opposed to running FLUSHALL through redis-cli"""
from argparse import ArgumentParser
from src.redis_common import RedisCommon

DEFAULT_REDIS_PORT = 6379


class RedisFlushall(RedisCommon):
    def __init__(self, host, port, cluster_mode, authentication, decode_responses, flushall, asynchronous):
        super().__init__(host, port, cluster_mode, authentication, decode_responses)
        self.redis_client = self.get_redis_client()
        self.flushall = flushall
        self.asynchronous = asynchronous

    def execute(self):
        if self.flushall:
            self.redis_client.flushall(asynchronous=self.asynchronous)


def get_arguments():
    parser = ArgumentParser(description="Run FLUSHALL - especially helpful for clustered Redis")
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
    parser.add_argument("--flushall", action="store_true", required=True, help="Confirmation to run FLUSHALL - required argument")
    parser.add_argument("--asynchronous", action="store_true", required=False, help="Run FLUSHALL asynchronously")
    return parser.parse_args()

def main():
    args = get_arguments()
    RedisFlushall(**vars(args))


if __name__ == "__main__":
    main()