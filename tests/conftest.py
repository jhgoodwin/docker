import os
import logging
from pytest import fixture, mark
#from dotenv import load_dotenv
#load_dotenv()

log_level = logging._nameToLevel[os.getenv("LOGLEVEL", "DEBUG")]
logging.basicConfig(format='%(levelname)s: %(message)s', level=log_level)
logging.debug('DEBUG logging enabled')

safe = mark.safe
unsafe = mark.unsafe


@fixture
def citus_master_postgres_url() -> str:
    url = os.getenv('DATABASE_URL', None)
    assert url is not None
    return url