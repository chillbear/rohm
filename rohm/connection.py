from redis import Redis

conn = Redis()


def get_connection():
    """ Get a connection """
    return conn
