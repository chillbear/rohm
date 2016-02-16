"""
Terminology notes: "connection" and "client" are synonymous here. Client is
probably more accurate technically (a redis-py Redis instance) but just using
connection for consistency
"""

from redis import StrictRedis

connection = None


def set_default_connection_settings(**kwargs):
    global connection

    set_default_connection(StrictRedis(**kwargs))


def set_default_connection(redis_client):
    global connection
    connection = redis_client


def get_default_connection():
    """ Get the default connection """
    global connection
    if not connection:
        connection = StrictRedis()

    return connection


def get_connection():
    """ Deprecated function """
    return get_default_connection()


def create_connection(**settings):
    return StrictRedis(**settings)
