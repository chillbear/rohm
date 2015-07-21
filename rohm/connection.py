from redis import StrictRedis

connection = None


def set_connection_settings(**kwargs):
    global connection

    set_client(StrictRedis(**kwargs))


def set_client(redis_client):
    from rohm import models
    global connection
    connection = redis_client
    models.conn = connection


def get_connection():
    """ Get a connection """
    global connection
    if not connection:
        connection = StrictRedis()

    return connection
