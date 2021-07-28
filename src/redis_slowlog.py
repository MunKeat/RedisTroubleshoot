import pytz

from argparse import ArgumentParser
from datetime import datetime
from pprint import pprint
from redis_common import RedisCommon


DEFAULT_REDIS_PORT = 6379


class RedisSlowlog(RedisCommon):
    MICROSECONDS_PER_MILLISECOND = 1000
    DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S%z"

    def __init__(self, host, port, cluster_mode, authentication, decode_responses, entries=None):
        super().__init__(host, port, cluster_mode, authentication, decode_responses)
        self.redis_client = self.get_redis_client()
        self.redis_pipeline = self.get_redis_pipeline()
        # Parameter entered
        self.entries = entries

    def cluster_mode_processing(self, logs):
        """Standardise format returned by differing Cluster Mode"""
        if isinstance(logs, dict):
            # Implies that this is obtained from Cluster Mode Enabled
            processed_logs = []
            for host, slowlog_entries in logs.items():
                [entry.update({"host": host}) for entry in slowlog_entries]
                processed_logs.extend(slowlog_entries)
            return processed_logs
        else:
            return logs

    def append_logs(self, logs):
        """Append log with readable information"""
        for log in logs:
            if log.get("start_time", None) is not None:
                start_time = int(log["start_time"])
                log["readable_start_time"] = (datetime.
                                              utcfromtimestamp(start_time).
                                              replace(tzinfo=pytz.utc).
                                              strftime(self.DEFAULT_DATETIME_FORMAT))
            if log.get("duration", None) is not None:
                log["duration_in_microsec"] = log.pop("duration")
                log["duration_in_millisec"] = int(log["duration_in_microsec"] / self.MICROSECONDS_PER_MILLISECOND)
        return logs

    def parse_string_codec(self, logs):
        for log in logs:
            log["command"] = str(log.get("command", ""), "utf-8")
        return logs

    def execute(self):
        # Get Redis slowlog
        logs = self.redis_client.slowlog_get(self.entries)
        logs = self.cluster_mode_processing(logs)
        logs = self.append_logs(logs)
        # Prevent parsing issue
        logs = self.parse_string_codec(logs)
        # Construct complete slowlogs
        slowlog_entries = sorted(logs, key=lambda x: x.get("start_time", ""), reverse=True)
        return slowlog_entries


def get_arguments():
    parser = ArgumentParser(description="Obtain slowlog from Redis instances")
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
    return parser.parse_args()


def main():
    args = get_arguments()
    task = RedisSlowlog(**vars(args))
    result = task.execute()
    pprint(result)


if __name__ == "__main__":
    main()
