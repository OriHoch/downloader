import psycopg2
import os
import psycopg2.extras
from psycopg2.errors import UniqueViolation


DSN = os.environ['DOWNLOADER_DB_DSN']


def execute(*args):
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(*args)
            conn.commit()


def rows_iterator(*args):
    with psycopg2.connect(DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(*args)
            for row in cur:
                yield row


def only_one(*args):
    row = None
    for rownum, row in enumerate(rows_iterator(*args)):
        if rownum > 0:
            raise Exception("Unexpected result from query")
    return row


def migrate():
    with open(os.path.join(os.path.dirname(__file__), 'schema.sql')) as f:
        execute(f.read())
