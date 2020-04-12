from . import db
from collections import OrderedDict


def create(name):
    db.execute('INSERT INTO app (name) VALUES (%s)', (name,))


def list_apps():
    for row in db.rows_iterator('select name from app order by name'):
        yield OrderedDict(
            name=row['name']
        )


def get_app_collections(app_name):
    for row in db.rows_iterator("""
                        select
                            app.id app_id,
                            collection.id collection_id,
                            collection.name collection_name
                        from
                            app
                            left join collection on collection.app_id = app.id
                        where
                            app.name = %s
                        order by collection.name
                    """, (app_name,)):
        cntrow = db.only_one('select count(1) cnt from collection_url where collection_id=%s', (row['collection_id'],))
        num_urls = cntrow['cnt'] if cntrow else 0
        yield OrderedDict(
            name=row['collection_name'],
            urls=num_urls
        )
