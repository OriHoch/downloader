from dataflows import Flow, update_resource, dump_to_path, load
import os
from collections import defaultdict
import tempfile
import datetime
import time
from . import db
import pycurl
import signal
from signal import SIGPIPE, SIG_IGN
import hashlib
import shutil


HASH_BLOCKSIZE = 65536
MAX_DOWNLOAD_RUNTIME_SECONDS = 60*30
DOWNLOAD_ITERATIONS_SLEEP_SECONDS = 2
DOWNLOAD_DOMAIN_THROTTLE_SECONDS = 5
DOWNLOAD_CONNECT_TIMEOUT = 30
DOWNLOAD_MAX_REDIRECTS = 5

MIN_TIMEOUT_SECONDS = 15
MAX_TIMEOUT_SECONDS = 300
MAX_SAMEDOMAINS = 50
RETRY_FAILED_MIN_SECONDS = 600

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

DAEMON_SLEEP_TIME_SECONDS = 60

DOWNLOAD_ITERATOR_DEFAULT_MIN_SAME_DOMAIN_BUCKET = 100  # domain bucket will be created for urls have more then 100 per domain
DOWNLOAD_ITERATOR_DEFAULT_MIN_LAST_UPDATE_SECONDS = 600  # it includes new urls and urls which last update was more then 10 minutes ago
DOWNLOAD_ITERATOR_DEFAULT_MIN_TIMEOUT_SECONDS = None
DOWNLOAD_ITERATOR_DEFAULT_MAX_TIMEOUT_SECONDS = 15  # will not fetch urls which previously timed out at more then 15 seconds


def fetch_all_collection_urls(queue_directory):
    app_stats = {}
    all_collection_ids = set()

    def _update_stats(row):
        app_stats.setdefault(row['app_name'], defaultdict(int))[row['collection_name']] += 1
        all_collection_ids.add(row['collection_id'])
        return row

    domain_stats = defaultdict(int)

    def _domain_stats(row):
        domain_stats[row['domain_id']] += 1

    Flow(
        (_update_stats({k: app[k] for k in [
            'app_id',
            'app_name',

            'collection_id',
            'collection_name',

            'url_id',
            'url',

            'domain_id',

            'update_freq_minutes',

            'updated_at',
            'last_update_hash_id',
            'last_update_hash',
            'last_update_hash_size_bytes',
            'last_update_hash_downloaded_at',
            'last_update_hash_download_path',
            'last_update_hash_error',
            'last_update_error_code',
            'last_update_timedout_seconds',

            'last_successful_updated_at',
            'last_successful_hash_id',
            'last_successful_hash',
            'last_successful_hash_size_bytes',
            'last_successful_hash_downloaded_at',
            'last_successful_hash_download_path',
        ]}) for app in db.rows_iterator("""
                select
                    app.id             app_id,
                    app.name           app_name,

                    collection.id      collection_id,
                    collection.name    collection_name,

                    url.id             url_id,
                    url.url            url,

                    domain.id         domain_id,

                    collection_url.update_freq_minutes        update_freq_minutes,

                    url_update_history.updated_at             updated_at,
                    url_update_history.hash_id                last_update_hash_id, 
                    last_update_hash.hash                     last_update_hash,
                    last_update_hash.size_bytes               last_update_hash_size_bytes, 
                    last_update_hash.downloaded_at            last_update_hash_downloaded_at,
                    last_update_hash.download_path            last_update_hash_download_path,
                    url_update_history.error                  last_update_hash_error,
                    url_update_history.error_code             last_update_error_code,
                    url_update_history.timedout_seconds       last_update_timedout_seconds,

                    url_successful_history.updated_at         last_successful_updated_at,
                    url_successful_history.hash_id            last_successful_hash_id, 
                    last_successful_update_hash.hash          last_successful_hash, 
                    last_successful_update_hash.size_bytes    last_successful_hash_size_bytes, 
                    last_successful_update_hash.downloaded_at last_successful_hash_downloaded_at,
                    last_successful_update_hash.download_path last_successful_hash_download_path
                from
                    collection_url
                    join collection on collection.id = collection_url.collection_id
                    join app on app.id = collection.app_id
                    join url on url.id = collection_url.url_id
                    join domain on domain.id = url.domain_id

                    left join url_last_update on url_last_update.url_id = url.id
                    left join url_update_history on url_update_history.id = url_last_update.url_update_history_id
                    left join hash last_update_hash on last_update_hash.id = url_update_history.hash_id

                    left join url_last_successful_update on url_last_successful_update.url_id = url.id
                    left join url_update_history url_successful_history on url_successful_history.id = url_last_successful_update.url_update_history_id
                    left join hash last_successful_update_hash on last_successful_update_hash.id = url_successful_history.hash_id
            """)),
        _domain_stats,
        update_resource('res_1', name='all_collection_urls', path='all_collection_urls.csv'),
        dump_to_path(os.path.join(queue_directory, 'all_collection_urls')),
    ).process()
    return app_stats, all_collection_ids, domain_stats


def filter_collection_urls(queue_directory, domain_stats,
                           min_timeout_seconds=None, max_timeout_seconds=None,
                           min_same_domains=None, max_same_domains=None,
                           min_last_update_seconds=None):
    def _filter_rows(rows):
        for row in rows:
            num_same_domain = domain_stats[row['domain_id']]
            timeout_seconds = row['last_update_timedout_seconds'] if row['last_update_timedout_seconds'] else 0
            last_update_seconds = (datetime.datetime.now() - datetime.datetime.strptime(row['updated_at'], DATETIME_FORMAT)).total_seconds() if row['updated_at'] else None
            if (
                    (min_timeout_seconds is None or timeout_seconds >= min_timeout_seconds)
                    and (max_timeout_seconds is None or timeout_seconds <= max_timeout_seconds)
                    and (min_same_domains is None or num_same_domain >= min_same_domains)
                    and (max_same_domains is None or num_same_domain <= max_same_domains)
                    and (last_update_seconds is None or min_last_update_seconds is None or last_update_seconds >= min_last_update_seconds)
            ):
                yield row

    Flow(
        load(os.path.join(queue_directory, 'all_collection_urls', 'datapackage.json')),
        _filter_rows,
        update_resource('collection_urls', name='filtered_collection_urls', path='filtered_collection_urls.csv'),
        dump_to_path(os.path.join(queue_directory, 'filtered_collection_urls')),
    ).process()


def create_bucket(queue_directory, bucket_name, filter_row):
    filename = os.path.join(queue_directory, 'buckets', bucket_name + '.txt')

    def _filter_rows(rows):
        for row in rows:
            if filter_row(row):
                yield row

    def _dump_to_path(rows):
        with open(filename, 'w') as f:
            for row in rows:
                 f.write("%s\n" % row['url_id'])

    Flow(
        load(os.path.join(queue_directory, 'filtered_collection_urls', 'datapackage.json')),
        _filter_rows,
        _dump_to_path
    ).process()
    return {'file': open(filename)}


def bucket_get_next(bucket):
    if bucket['file'] is not None:
        line = bucket['file'].readline()
        if line == '':
            close_bucket(bucket)
            bucket['file'] = None
            return None
        else:
            return int(line.strip())
    else:
        return None


def close_bucket(bucket):
    if bucket['file'] is not None:
        bucket['file'].close()


def filter_bucket_type(bucket_type, row):
    if row['updated_at']:
        # url is not new
        if bucket_type == 'new':
            return False
        elif row['last_update_hash_id']:
            # last update was successful
            if bucket_type == 'update':
                return (
                        row['last_successful_updated_at']
                        and row['update_freq_minutes']
                        and (datetime.datetime.now() - datetime.datetime.strptime(row['last_successful_updated_at'], DATETIME_FORMAT)).total_seconds() / 60 > row['update_freq_minutes']
                )
            else:
                return False
        else:
            # last update was not successful
            return bucket_type == 'failed'
    else:
        # url is new
        return bucket_type == 'new'


def get_download_iterator(min_same_domain_bucket=DOWNLOAD_ITERATOR_DEFAULT_MIN_SAME_DOMAIN_BUCKET,
                          min_timeout_seconds=DOWNLOAD_ITERATOR_DEFAULT_MIN_TIMEOUT_SECONDS,
                          max_timeout_seconds=DOWNLOAD_ITERATOR_DEFAULT_MAX_TIMEOUT_SECONDS,
                          min_last_update_seconds=DOWNLOAD_ITERATOR_DEFAULT_MIN_LAST_UPDATE_SECONDS):

    def _get_urlobj(bucket, bucket_type):
        url_id = bucket_get_next(bucket)
        now = datetime.datetime.now()
        try:
            db.execute('insert into queue (url_id, timeout_seconds, added_at, status) values (%s, %s, %s, %s)',
                       (url_id, max_timeout_seconds, now.strftime(DATETIME_FORMAT), 'added'))
        except db.UniqueViolation:
            pass
        row = db.only_one('select id, timeout_seconds, added_at, status from queue where url_id=%s', (url_id,))
        if row['status'] == 'done' or ((now - datetime.datetime.strptime(row['added_at'], DATETIME_FORMAT)).total_seconds() > row['timeout_seconds'] + 600 and row['status'] != 'added'):
            db.execute('update queue set status=%s where url_id=%s', ('added', url_id))



    def _iterator():
        while True:
            with tempfile.TemporaryDirectory() as queue_directory:
                app_stats, all_collection_ids, domain_stats = fetch_all_collection_urls(queue_directory)
                filter_collection_urls(queue_directory, domain_stats, min_timeout_seconds=min_timeout_seconds, max_timeout_seconds=max_timeout_seconds,
                                       min_last_update_seconds=min_last_update_seconds)
                buckets_same_domain_ids = set()
                for domain_id, num_urls in domain_stats.items():
                    if num_urls >= min_same_domain_bucket:
                        buckets_same_domain_ids.add(domain_id)
                buckets = {}
                try:
                    for bucket_type in ['new', 'update', 'failed']:
                        for domain_id in buckets_same_domain_ids:
                            bucket_name = os.path.join(bucket_type, 'domain_%s') % domain_id
                            buckets[bucket_name] = create_bucket(queue_directory, bucket_name, lambda row: row['domain_id'] == domain_id and filter_bucket_type(bucket_type, row))
                        for collection_id in all_collection_ids:
                            bucket_name = os.path.join(bucket_type, 'collection_%s') % collection_id
                            buckets[bucket_name] = create_bucket(queue_directory, bucket_name, lambda row: row['collection_id'] == collection_id and row['domain_id'] not in buckets_same_domain_ids and filter_bucket_type(bucket_type, row))
                    for bucket_type in ['new', 'update', 'failed']:
                        for domain_id in buckets_same_domain_ids:
                            bucket_name = os.path.join(bucket_type, 'domain_%s') % domain_id
                            urlobj = _get_urlobj(buckets[bucket_name], bucket_type)
                            if urlobj:
                                yield urlobj
                        for collection_id in all_collection_ids:
                            bucket_name = os.path.join(bucket_type, 'collection_%s') % collection_id
                            urlobj = _get_urlobj(buckets[bucket_name], bucket_type)
                            if urlobj:
                                yield urlobj
                finally:
                    for bucket in buckets.values():
                        close_bucket(bucket)

    return _iterator


def fetch(queue_type, queue_directory):
    if os.path.exists(queue_directory):
        raise Exception("queue directory already exists, delete it to continue (%s)" % queue_directory)
    os.mkdir(queue_directory)
    app_stats, all_collection_ids, domain_stats = fetch_all_collection_urls(queue_directory)
    if queue_type == 'timedout':
        filter_collection_urls(queue_directory, domain_stats, min_timeout_seconds=MIN_TIMEOUT_SECONDS, max_timeout_seconds=MAX_TIMEOUT_SECONDS)
    elif queue_type == 'samedomain':
        filter_collection_urls(queue_directory, domain_stats, min_same_domains=MAX_SAMEDOMAINS)
    else:
        filter_collection_urls(queue_directory, domain_stats, max_timeout_seconds=MIN_TIMEOUT_SECONDS, max_same_domains=MAX_SAMEDOMAINS)

    for bucket_type in ['new', 'update', 'failed']:
        os.makedirs(os.path.join(queue_directory, 'buckets', bucket_type), exist_ok=True)
        for collection_id in all_collection_ids:
            with open(os.path.join(queue_directory, 'buckets', bucket_type, str(collection_id) + '.txt'), 'w') as f:

                def fill_bucket(row):
                    ok_to_write = False
                    if row['collection_id'] == collection_id and row['url_id']:
                        if bucket_type == 'new':
                            if row['updated_at'] is None:
                                ok_to_write = True
                        else:
                            last_failed = False
                            last_failed_more_then_10_minutes = False
                            num_consecutive_failures = 0
                            if row['last_update_hash_id'] is None:
                                last_failed = True
                                if (datetime.datetime.now() - datetime.datetime.strptime(row['updated_at'], DATETIME_FORMAT)).total_seconds() > RETRY_FAILED_MIN_SECONDS:
                                    last_failed_more_then_10_minutes = True
                                    for tmprow in db.rows_iterator(
                                        "select hash_id from url_update_history where url_id=%s order by updated_at desc limit 5",
                                        (row['url_id'],)
                                    ):
                                        if tmprow['hash_id'] is None:
                                            num_consecutive_failures += 1
                                        else:
                                            break
                            is_failed_bucket = last_failed and last_failed_more_then_10_minutes and num_consecutive_failures
                            if bucket_type == 'failed':
                                if is_failed_bucket:
                                    ok_to_write = True
                            elif not is_failed_bucket:
                                if (
                                        row['last_successful_updated_at']
                                        and row['update_freq_minutes']
                                        and (datetime.datetime.now() - datetime.datetime.strptime(row['last_successful_updated_at'], DATETIME_FORMAT)).total_seconds() / 60 > row['update_freq_minutes']
                                ):
                                    ok_to_write = True
                    if ok_to_write:
                        f.write('%s %s\n' % (row['url_id'], row['url']))

                Flow(
                    load(os.path.join(queue_directory, 'filtered_collection_urls', 'datapackage.json')),
                    fill_bucket
                ).process()
    bucket_files = {}
    all_url_ids = set()
    try:
        for bucket_type in ['new', 'update', 'failed']:
            for collection_id in all_collection_ids:
                bucket_files[bucket_type + str(collection_id)] = open(os.path.join(queue_directory, 'buckets', bucket_type, str(collection_id) + '.txt'))
        with open(os.path.join(queue_directory, 'queue.txt'), 'w') as f:
            while len(bucket_files) > 0:
                for key in list(bucket_files.keys()):
                    line = bucket_files[key].readline()
                    if line == '':
                        bucket_files[key].close()
                        del bucket_files[key]
                    else:
                        url_id = int(line.split(' ')[0])
                        if url_id not in all_url_ids:
                            all_url_ids.add(url_id)
                            f.write(line)
    finally:
        for file in bucket_files.values():
            file.close()
    return app_stats, len(all_url_ids)


def download(queue_type, queue_directory, output_directory, concurrent_connections, max_downloads=None):
    if queue_type == 'timedout':
        timeout_seconds = MAX_TIMEOUT_SECONDS
    else:
        timeout_seconds = MIN_TIMEOUT_SECONDS
    # We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
    # the libcurl tutorial for more info.
    signal.signal(SIGPIPE, SIG_IGN)
    m = pycurl.CurlMulti()
    m.handles = []
    for i in range(int(concurrent_connections)):
        c = pycurl.Curl()
        c.fp = None
        c.setopt(pycurl.FOLLOWLOCATION, 1)
        c.setopt(pycurl.MAXREDIRS, DOWNLOAD_MAX_REDIRECTS)
        c.setopt(pycurl.CONNECTTIMEOUT, DOWNLOAD_CONNECT_TIMEOUT)
        c.setopt(pycurl.TIMEOUT, int(timeout_seconds))
        c.setopt(pycurl.NOSIGNAL, 1)
        m.handles.append(c)
    start_time = datetime.datetime.now()
    try:
        while True:
            num_processed = 0
            reached_max_downloads = False
            total_read_lines = 0
            domains_last_start_times = {}
            skipped_due_to_domain_start_time = 0
            total_stats = {
                'num_existing_hash_id': 0,
                'num_new_hash_id': 0,
                'num_error_urls': 0,
                'num_timeout_urls': 0
            }
            freelist = m.handles[:]
            eof = False
            downloaded_url_ids = set()
            if os.path.exists(os.path.join(queue_directory, 'output.txt')):
                with open(os.path.join(queue_directory, 'output.txt')) as f:
                    for line in f:
                        downloaded_url_ids.add(int(line.strip()))
            with open(os.path.join(queue_directory, 'output.txt'), 'a') as output_file:
                with tempfile.TemporaryDirectory() as tmpdir:

                    def save_result(url, url_id, errno=None, errmsg=None, response_code=None):
                        is_timeout = errno == pycurl.E_OPERATION_TIMEDOUT
                        now = datetime.datetime.now()
                        url_relative_output_dir = os.path.join(
                            str(now.year), str(now.month), str(now.day), str(now.hour), str(now.minute), str(url_id)
                        )
                        url_output_dir = os.path.join(output_directory, url_relative_output_dir)
                        url_relative_output_filename = os.path.join(url_relative_output_dir, "output")
                        output_filename = os.path.join(tmpdir, str(url_id), "output")
                        header_filename = os.path.join(tmpdir, str(url_id), "header")
                        hash_id = None
                        if errno is None and response_code == 200:
                            filesize = os.path.getsize(output_filename)
                            if filesize > 0:
                                hasher = hashlib.sha256()
                                with open(output_filename, 'rb') as f:
                                    buf = f.read(HASH_BLOCKSIZE)
                                    while len(buf) > 0:
                                        hasher.update(buf)
                                        buf = f.read(HASH_BLOCKSIZE)
                                hash = hasher.hexdigest()
                                try:
                                    db.execute(
                                        "insert into hash (hash, size_bytes, download_path, downloaded_at) values (%s, %s, %s, %s)",
                                        (hash, filesize, url_relative_output_filename, datetime.datetime.now().strftime(DATETIME_FORMAT))
                                    )
                                    total_stats['num_new_hash_id'] += 1
                                    os.makedirs(url_output_dir)
                                    os.rename(output_filename, os.path.join(output_directory, url_relative_output_filename))
                                except db.UniqueViolation:
                                    total_stats['num_existing_hash_id'] += 1
                                    os.unlink(output_filename)
                                hash_id = db.only_one("select id from hash where hash=%s and size_bytes=%s", (hash, filesize))['id']
                        else:
                            if is_timeout:
                                total_stats['num_timeout_urls'] += 1
                            else:
                                total_stats['num_error_urls'] += 1
                            os.unlink(output_filename)
                        os.unlink(header_filename)
                        os.rmdir(os.path.join(tmpdir, str(url_id)))
                        url_update_history_id = db.only_one(
                            "insert into url_update_history (url_id, updated_at, hash_id, error, error_code, timedout_seconds) values (%s, %s, %s, %s, %s, %s) RETURNING id",
                            (url_id, datetime.datetime.now().strftime(DATETIME_FORMAT), hash_id, errmsg, errno or response_code, timeout_seconds if is_timeout else None)
                        )['id']
                        try:
                            db.execute("insert into url_last_update (url_id, url_update_history_id) values (%s, %s)", (url_id, url_update_history_id))
                        except db.UniqueViolation:
                            db.execute("update url_last_update set url_update_history_id=%s where url_id=%s", (url_update_history_id, url_id))
                        if hash_id:
                            try:
                                db.execute("insert into url_last_successful_update (url_id, url_update_history_id) values (%s, %s)", (url_id, url_update_history_id))
                            except db.UniqueViolation:
                                db.execute("update url_last_successful_update set url_update_history_id=%s where url_id=%s", (url_update_history_id, url_id))
                        output_file.write(str(url_id) + "\n")

                    with open(os.path.join(queue_directory, 'queue.txt')) as queue_file:
                        while True:
                            while freelist and not eof:
                                line = queue_file.readline()
                                if line == '':
                                    eof = True
                                else:
                                    total_read_lines += 1
                                    tmp = line.strip().split(" ")
                                    url_id, url = tmp[0], ' '.join(tmp[1:])
                                    if int(url_id) not in downloaded_url_ids:
                                        domain = url.split('://')[1].split('/')[0]
                                        domain_last_start_time = domains_last_start_times.get(domain, None)
                                        now = datetime.datetime.now()
                                        if not domain_last_start_time or (now - domain_last_start_time).total_seconds() >= DOWNLOAD_DOMAIN_THROTTLE_SECONDS:
                                            domains_last_start_times[domain] = now
                                            c = freelist.pop()
                                            os.mkdir(os.path.join(tmpdir, str(url_id)))
                                            c.fp = open(os.path.join(tmpdir, str(url_id), "output"), "wb")
                                            c.hfp = open(os.path.join(tmpdir, str(url_id), "header"), "wb")
                                            c.setopt(pycurl.URL, url)
                                            c.setopt(pycurl.WRITEDATA, c.fp)
                                            c.setopt(pycurl.WRITEHEADER, c.hfp)
                                            m.add_handle(c)
                                            c.url_id = url_id
                                            c.url = url
                                        else:
                                            skipped_due_to_domain_start_time += 1
                            while True:
                                ret, num_handles = m.perform()
                                if ret != pycurl.E_CALL_MULTI_PERFORM:
                                    break
                            while True:
                                num_q, ok_list, err_list = m.info_read()
                                for c in ok_list:
                                    c.fp.close()
                                    c.fp = None
                                    c.hfp.close()
                                    c.hfp = None
                                    m.remove_handle(c)
                                    save_result(c.url, c.url_id, response_code=c.getinfo(pycurl.RESPONSE_CODE))
                                    freelist.append(c)
                                for c, errno, errmsg in err_list:
                                    c.fp.close()
                                    c.fp = None
                                    c.hfp.close()
                                    c.hfp = None
                                    m.remove_handle(c)
                                    save_result(c.url, c.url_id, errno=errno, errmsg=errmsg)
                                    freelist.append(c)
                                num_processed = num_processed + len(ok_list) + len(err_list)
                                if max_downloads and num_processed >= int(max_downloads):
                                    reached_max_downloads = True
                                    break
                                if num_q == 0:
                                    break
                            if num_q == 0 and num_handles == 0 and eof:
                                break
                            if max_downloads and num_processed >= int(max_downloads):
                                reached_max_downloads = True
                                break
                            m.select(1.0)
            if len(downloaded_url_ids) == total_read_lines:
                break
            elif (start_time - datetime.datetime.now()).total_seconds() > MAX_DOWNLOAD_RUNTIME_SECONDS:
                break
            else:
                time.sleep(DOWNLOAD_ITERATIONS_SLEEP_SECONDS)
    finally:
        for c in m.handles:
            if getattr(c, 'fp', None) is not None:
                c.fp.close()
                c.fp = None
            if getattr(c, 'hfp', None) is not None:
                c.hfp.close()
                c.hfp = None
            c.close()
        m.close()
    return (
        len(downloaded_url_ids), num_processed, reached_max_downloads, total_read_lines, skipped_due_to_domain_start_time,
        total_stats['num_existing_hash_id'], total_stats['num_new_hash_id'], total_stats['num_error_urls'], total_stats['num_timeout_urls']
    )


def daemon(queue_type, queue_directory, output_directory, concurrent_connections):
    if queue_type not in ['samedomain', 'timedout', 'regular']:
        raise Exception('invalid queue_type: ' + queue_type)
    elif os.path.exists(queue_directory):
        raise Exception('queue_directory already exists: ' + queue_directory)
    else:
        try:
            while True:
                app_stats, len_all_url_ids = fetch(queue_type, queue_directory)
                print('fetched ' + str(len_all_url_ids) + ' urls')
                (
                    num_already_downloaded, num_processed, reached_max_downloads, total_read_lines, skipped_due_to_domain_start_time,
                    num_existing_hash_id, num_new_hash_id, num_error_urls, num_timeout_urls
                ) = download(queue_type, queue_directory, output_directory, concurrent_connections)
                print('downloaded ' + str(total_read_lines) + ' urls')
                shutil.rmtree(queue_directory)
                time.sleep(DAEMON_SLEEP_TIME_SECONDS)
        finally:
            if os.path.exists(queue_directory):
                shutil.rmtree(queue_directory)
