from redis import StrictRedis

# conn = Redis()
connection = None


def setup_connection(redis_client):
    global connection
    connection = redis_client


def get_connection():
    """ Get a connection """
    global connection
    if not connection:
        connection = StrictRedis()

    return connection
