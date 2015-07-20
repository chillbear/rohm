from redis import StrictRedis

# conn = Redis()
connection = None


def set_connection_settings(**kwargs):
    global connection

    # settings = {}
    # if host:
    #     settings['host'] = host
    # if port:
    #     settings['port'] = port
    # if password:
    #     settings['password']


    # connection = StrictRedis(host=host, port=port, password=password, db=db)
    connection = StrictRedis(**kwargs)


def set_client(redis_client):
    global connection
    connection = redis_client


def get_connection():
    """ Get a connection """
    global connection
    if not connection:
        connection = StrictRedis()

    return connection
