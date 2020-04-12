import json
from . import db
from collections import OrderedDict
from . import user
import os


GET_URL_MAX_HISTORY_ITEMS = 100
GET_URL_MAX_TAGS = 100
SEARCH_URL_MAX_HISTORY_ITEMS = 10
SEARCH_URL_MAX_TAGS = 10


class UrlOrTitleAlreadyExistsInCollection(Exception):
    pass


def add(app_name, url, extra_json=None, verify_username=None):
    if extra_json is None:
        extra = {}
    else:
        extra = json.loads(extra_json)
    app_name = app_name.strip()
    row = db.only_one('SELECT id FROM app WHERE name = %s', (app_name,))
    if not row:
        raise Exception('invalid app name')
    app_id = row['id']
    user.verify_user_app(verify_username, app_id)
    url = url.strip()
    title = extra.pop('title', url).strip()
    collection = extra.pop('collection', 'default').strip()
    tags = extra.pop('tags', {})
    metadata = extra.pop('metadata', {})
    update_freq_minutes = extra.pop('update_freq_minutes', 0)
    if len(url) < 10:
        raise Exception('url length must be at least 10 characters')
    elif "\n" in url or "\r" in url:
        raise Exception("Invalid characters in URL")
    elif not url.startswith('http://') and not url.startswith('https://'):
        raise Exception("Invalid URL schema")
    elif not isinstance(title, str):
        raise Exception('title must be a string')
    elif not isinstance(collection, str):
        raise Exception('collection must be a string')
    elif not isinstance(tags, dict):
        raise Exception('tags must be an object')
    elif not isinstance(metadata, dict):
        raise Exception('metadata must be an object')
    elif not isinstance(update_freq_minutes, int):
        raise Exception('update_freq_minutes must be an integer')
    elif not all((isinstance(v, str) for k,v in tags.items())):
        raise Exception('all tags must be a string')
    else:
        domain = url.split('://')[1].split('/')[0]
        try:
            db.execute('insert into domain (domain) values (%s)', (domain,))
        except db.UniqueViolation:
            pass
        domain_id = db.only_one('select id from domain where domain=%s', (domain,))['id']
        try:
            db.execute('insert into url (url, domain_id) values (%s, %s)', (url, domain_id))
        except db.UniqueViolation:
            pass
        row = db.only_one('select id from url where url=%s', (url,))
        url_id = row['id']
        try:
            db.execute('insert into collection (app_id, name) values (%s, %s)', (app_id, collection))
        except db.UniqueViolation:
            pass
        row = db.only_one('select id from collection where app_id=%s and name=%s', (app_id, collection))
        collection_id = row['id']
        try:
            db.execute(
                'insert into collection_url (collection_id, url_id, title, metadata, update_freq_minutes) values (%s, %s, %s, %s, %s)',
                (collection_id, url_id, title, json.dumps(metadata), update_freq_minutes)
            )
        except db.UniqueViolation:
            raise UrlOrTitleAlreadyExistsInCollection('URL or URL title already exists in the collection')
        row = db.only_one('select id from collection_url where collection_id=%s and url_id=%s', (collection_id, url_id))
        collection_url_id = row['id']
        db.execute('delete from url_tag where collection_url_id=%s', (collection_url_id,))
        for tag, value in tags.items():
            if not value:
                continue
            try:
                db.execute('insert into tag (name) values (%s)', (tag,))
            except db.UniqueViolation:
                pass
            row = db.only_one('select id from tag where name=%s', (tag,))
            tag_id = row['id']
            db.execute('insert into url_tag (collection_url_id, tag_id, value) values (%s, %s, %s)', (collection_url_id, tag_id, value))
        return {'id': collection_url_id}


def edit(collection_url_id, update_json, verify_username=None):
    update = json.loads(update_json)
    new_title = update.pop('title', None)
    new_collection = update.pop('collection', None)
    new_tags = update.pop('tags', None)
    new_metadata = update.pop('metadata', None)
    new_update_freq_minutes = update.pop('update_freq_minutes', None)
    if new_title is not None:
        new_title = new_title.strip()
    if new_collection is not None:
        raise Exception('cannot change collection')
    if new_title is not None and not isinstance(new_title, str):
        raise Exception('new title must be a string')
    elif new_collection is not None and not isinstance(new_collection, str):
        raise Exception('new collection must be a string')
    elif new_tags is not None and not isinstance(new_tags, dict):
        raise Exception('new tags must be an object')
    elif new_metadata is not None and not isinstance(new_metadata, dict):
        raise Exception('new metadata must be an object')
    elif new_update_freq_minutes is not None and not isinstance(new_update_freq_minutes, int):
        raise Exception('new update_freq_minutes must be an integer')
    elif new_tags is not None and not all((isinstance(v, str) for k,v in new_tags.items())):
        raise Exception('all tags must be a string')
    else:
        try:
            row = db.only_one('select collection.app_id from collection_url, collection where collection_url.id=%s and collection_url.collection_id=collection.id', (collection_url_id,))
            app_id = row['app_id']
        except Exception:
            raise Exception('invalid id')
        user.verify_user_app(verify_username, app_id)
        if new_title:
            try:
                db.execute('update collection_url set title=%s where id=%s', (new_title, collection_url_id))
            except db.UniqueViolation:
                raise UrlOrTitleAlreadyExistsInCollection('new title already exists in collection')
        if new_metadata:
            db.execute('update collection_url set metadata=%s where id=%s', (json.dumps(new_metadata), collection_url_id))
        if new_update_freq_minutes:
            db.execute('update collection_url set update_freq_minutes=%s where id=%s', (new_update_freq_minutes, collection_url_id))
        if new_tags:
            db.execute('delete from url_tag where collection_url_id=%s', (collection_url_id,))
            for tag, value in new_tags.items():
                if not value: continue
                try:
                    db.execute('insert into tag (name) values (%s)', (tag,))
                except db.UniqueViolation:
                    pass
                row = db.only_one('select id from tag where name=%s', (tag,))
                tag_id = row['id']
                db.execute('insert into url_tag (collection_url_id, tag_id, value) values (%s, %s, %s)', (collection_url_id, tag_id, value))


def _get_url(row, max_history_rows, max_tags_rows):
    collection_url_id = row['collection_url_id']
    url_id = row['url_id']
    url = OrderedDict(
        app=row['app_name'],
        collection=row['collection_name'],
        url=row['url'],
        title=row['title'],
        metadata=json.loads(row['metadata']),
        update_freq_minutes=row['update_freq_minutes'],
        downloaded_at=row['last_successfull_downloaded_at'] if row['last_update_hash_id'] else None,
        download_url=os.environ.get('OUTPUT_URL_PREFIX', '').strip('/') + '/' + row['last_successfull_download_path'] if
        row['last_update_hash_id'] else None,
        downloaded_hash=row['last_successfull_hash'] if row['last_update_hash_id'] else None,
        downloaded_size_bytes=row['last_successfull_size_bytes'] if row['last_update_hash_id'] else None,
        last_error_at=row['last_updated_at'] if not row['last_update_hash_id'] else None,
        last_error=row['last_error'] if not row['last_update_hash_id'] else None,
        last_error_code=row['last_error_code'] if not row['last_update_hash_id'] else None,
        last_error_timedout_seconds=row['last_timedout_seconds'] if not row['last_update_hash_id'] else None
    )
    if max_history_rows > 0:
        url['history'] = []
        for row in db.rows_iterator("""
                                select
                                    url_update_history.updated_at,
                                    url_update_history.timedout_seconds,
                                    url_update_history.error_code,
                                    url_update_history.error,
                                    hash.hash,
                                    hash.downloaded_at,
                                    hash.download_path,
                                    hash.size_bytes
                                from
                                    url_update_history
                                    left join hash on hash.id = url_update_history.hash_id
                                where
                                    url_update_history.url_id=%s
                                order by url_update_history.updated_at desc
                                limit %s
                            """, (url_id, max_history_rows)):
            url['history'].append(OrderedDict(
                updated_at=row['updated_at'],
                download_path=row['download_path'],
                size_bytes=row['size_bytes'],
                hash=row['hash'],
                timedout_seconds=row['timedout_seconds'],
                error_code=row['error_code'],
                error=row['error']
            ))
    if max_tags_rows > 0:
        url['tags'] = OrderedDict()
        for row in db.rows_iterator("""
                                select tag.name, url_tag.value
                                from url_tag join tag on tag.id = url_tag.tag_id
                                where url_tag.collection_url_id = %s
                                order by tag.name
                                limit %s
                            """, (collection_url_id, max_tags_rows)):
            url['tags'][row['name']] = row['value']
    return url


def get_urls(collection_url_id=None, app_name=None, collection_name=None, title=None, title_startswith=None, title_contains=None, tag=None, with_history=False, with_tags=False, verify_username=None):
    if collection_url_id is not None:
        if app_name is not None or collection_name is not None or title is not None or title_startswith is not None or title_contains is not None or tag is not None:
            raise Exception('invalid arguments')
        is_single = True
        collection_url_from = ''
        collection_url_where = 'collection_url.id = %s'
        collection_url_values = (collection_url_id,)
    else:
        if app_name is None or collection_name is None:
            raise Exception('invalid arguments')
        row = db.only_one("""
            select app.id app_id, collection.id collection_id
            from app, collection
            where app.id = collection.app_id
            and app.name = %s
            and collection.name = %s
        """, (app_name, collection_name))
        if not row:
            raise Exception('invalid arguments')
        user.verify_user_app(verify_username, row['app_id'])
        collection_id = row['collection_id']
        collection_url_where = 'collection_id=%s'
        collection_url_values = [collection_id]
        if title:
            if title_startswith is not None or title_contains is not None or tag is not None:
                raise Exception('invalid arguments')
            is_single = False
            collection_url_from = ''
            collection_url_where += ' and collection_url.title = %s'
            collection_url_values.append(title.strip())
        elif title_startswith:
            if title_contains is not None or tag is not None:
                raise Exception('invalid arguments')
            is_single = False
            collection_url_from = ''
            collection_url_where += ' and collection_url.title like %s'
            collection_url_values.append(title_startswith.strip() + '%')
        elif title_contains:
            if tag is not None:
                raise Exception('invalid arguments')
            is_single = False
            collection_url_from = ''
            collection_url_where += ' and collection_url.title like %s'
            collection_url_values.append('%' + title_contains.strip() + '%')
        elif tag:
            tag_name, tag_value = tag.split('=')
            row = db.only_one('select id from tag where name=%s', (tag_name,))
            if not row:
                raise Exception('no such tag: {}'.format(tag_name))
            tag_id = row['id']
            is_single = False
            collection_url_from = 'join url_tag on url_tag.collection_url_id = collection_url.id'
            collection_url_where += ' and url_tag.tag_id=%s and url_tag.value=%s'
            collection_url_values.append(tag_id)
            collection_url_values.append(tag_value)
        else:
            is_single = False
            collection_url_from = ''
    collection_url_sql = """
        select
            collection_url.id collection_url_id,
            app.id app_id,
            app.name app_name,
            collection.name collection_name,
            collection_url.title,
            collection_url.metadata,
            collection_url.update_freq_minutes,
            url.url,
            url.id url_id,
            last_update_history.hash_id last_update_hash_id,
            last_update_history.updated_at last_updated_at,
            last_update_history.error last_error,
            last_update_history.error_code last_error_code,
            last_update_history.timedout_seconds last_timedout_seconds,
            last_successfull_update_hash.size_bytes last_successfull_size_bytes,
            last_successfull_update_hash.download_path last_successfull_download_path,
            last_successfull_update_hash.downloaded_at last_successfull_downloaded_at,
            last_successfull_update_hash.hash last_successfull_hash
        from
            collection_url
            join collection on collection.id = collection_url.collection_id
            join app on app.id = collection.app_id
            join url on url.id = collection_url.url_id
            left join url_last_update on url_last_update.url_id = url.id
            left join url_update_history last_update_history on last_update_history.id = url_last_update.url_update_history_id
            left join url_last_successful_update on url_last_successful_update.url_id = url.id
            left join url_update_history last_successfull_update_history on last_successfull_update_history.id = url_last_successful_update.url_update_history_id
            left join hash last_successfull_update_hash on last_successfull_update_hash.id = last_successfull_update_history.hash_id
            {}
        where
            {}
    """.format(collection_url_from, collection_url_where)
    rows = db.rows_iterator(collection_url_sql, collection_url_values)
    if is_single:
        rows = list(rows)
        if len(rows) != 1:
            raise Exception('failed to find url')
        row = rows[0]
        user.verify_user_app(verify_username, row['app_id'])
        yield _get_url(row, GET_URL_MAX_HISTORY_ITEMS if with_history else 0, GET_URL_MAX_TAGS if with_tags else 0)
    else:
        for row in rows:
            yield _get_url(row, SEARCH_URL_MAX_HISTORY_ITEMS if with_history else 0, SEARCH_URL_MAX_TAGS if with_tags else 0)
