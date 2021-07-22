# -*- coding: utf-8 -*-
from redis import Redis
from rediscluster import RedisCluster


class RedisCommon:
    DEFAULT_PORT = 6379
    DEFAULT_MAX_CONNECTION = 10
    DEFAULT_CONN_TIMEOUT = 10

    SECONDS_PER_MINUTE = 60

    def __init__(self, host, port=DEFAULT_PORT, cluster_mode=None, authentication=None, decode_responses=True):
        # Begin initialising for Redis Client
        if not cluster_mode:
            redis_client_args = {
                "host": host,
                "port": port,
                "socket_connect_timeout": self.DEFAULT_CONN_TIMEOUT
            }
            self.__redis_client = Redis(**redis_client_args)
        elif cluster_mode:
            redis_client_args = {
                "startup_nodes": [{"host": host, "port": port}],
                "decode_responses": decode_responses,
                "max_connections": self.DEFAULT_MAX_CONNECTION,
                "socket_connect_timeout": self.DEFAULT_CONN_TIMEOUT,
                "skip_full_coverage_check": True
            }
            self.__redis_client = RedisCluster(**redis_client_args)
        else:
            raise ValueError("Expect Boolean value for cluster_mode")
        # Create pipeline
        self.__redis_pipeline = self.get_redis_client().pipeline()
        # Initialise remaining variables
        self.__host = host
        self.__port = port
        self.__is_cluster = cluster_mode

    @property
    def host(self):
        """Getter method for host name"""
        return self.__host

    @property
    def port(self):
        """Getter method for port"""
        return self.__port

    @property
    def is_cluster(self):
        return self.__is_cluster

    def get_redis_client(self):
        return self.__redis_client

    def get_redis_pipeline(self):
        return self.__redis_pipeline
