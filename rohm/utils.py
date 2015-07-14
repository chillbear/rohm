from datetime import datetime

from pytz import utc


def utcnow():
    return utc.localize(datetime.now())
