import click
from ruamel.yaml.representer import RoundTripRepresenter
import ruamel.yaml
from collections import OrderedDict
import sys


class MyRepresenter(RoundTripRepresenter):
    pass


ruamel.yaml.add_representer(OrderedDict, MyRepresenter.represent_dict, representer=MyRepresenter)


def _yaml_dump_iterator(iterator):
    yaml = ruamel.yaml.YAML()
    yaml.Representer = MyRepresenter
    for obj in iterator:
        yaml.dump([obj], sys.stdout)
        sys.stdout.flush()


def _yaml_dump_object(obj):
    yaml = ruamel.yaml.YAML()
    yaml.Representer = MyRepresenter
    yaml.dump(obj, sys.stdout)
    sys.stdout.flush()


@click.group()
def main():
    pass


from . import db


@main.group("db")
def db_group():
    pass


@db_group.command("migrate")
def db_migrate():
    db.migrate()
    print('Successfully migrated the DB')


from . import app


@main.group("app")
def app_group():
    pass


@app_group.command("create")
@click.argument('APP_NAME')
def app_create(app_name):
    app.create(app_name)
    print("Successfully created app %s" % app_name)


@app_group.command("list")
def app_list():
    _yaml_dump_iterator(app.list_apps())


@app_group.command("get-collections")
@click.argument('APP_NAME')
def app_get_collections(app_name):
    _yaml_dump_iterator(app.get_app_collections(app_name))


from . import url as url_lib


@main.group('url')
def url_group():
    pass


@url_group.command("add")
@click.argument('APP_NAME')
@click.argument('URL')
@click.argument('EXTRA_JSON', required=False)
def url_add(app_name, url, extra_json):
    id = url_lib.add(app_name, url, extra_json)['id']
    print("Successfully added URL %s to app %s" % (url, app_name))
    print("ID=%s" % id)


@url_group.command("edit")
@click.argument('APP_NAME')
@click.argument('COLLECTION_NAME')
@click.argument('URL')
@click.argument('UPDATE_JSON')
def url_edit(app_name, collection_name, url, update_json):
    url_lib.edit(app_name, collection_name, url, update_json)
    print("Successfully edited URL %s in app %s" % (url, app_name))


@url_group.command("get")
@click.argument("ID")
@click.option("--with-history", is_flag=True)
@click.option("--with-tags", is_flag=True)
def url_get(id, with_history, with_tags):
    _yaml_dump_object(list(url_lib.get_urls(collection_url_id=id, with_history=with_history, with_tags=with_tags))[0])


@url_group.command("search")
@click.argument('APP_NAME')
@click.argument('COLLECTION_NAME')
@click.option('--title')
@click.option('--title-startswith')
@click.option('--title-contains')
@click.option('--tag')
@click.option("--with-history", is_flag=True)
@click.option("--with-tags", is_flag=True)
def url_search(app_name, collection_name, title, title_startswith, title_contains, tag, with_history, with_tags):
    _yaml_dump_iterator(url_lib.get_urls(app_name=app_name, collection_name=collection_name, title=title, title_startswith=title_startswith, title_contains=title_contains, tag=tag, with_history=with_history, with_tags=with_tags))


from . import queue


@main.group('queue')
def queue_group():
    pass


@queue_group.command('daemon')
@click.argument('QUEUE_TYPE')
@click.argument('QUEUE_DIRECTORY')
@click.argument('OUTPUT_DIRECTORY')
@click.argument('CONCURRENT_CONNECTIONS')
def queue_daemon(queue_type, queue_directory, output_directory, concurrent_connections):
    print('starting daemon')
    print('queue_type=' + queue_type)
    print('queue_directory=' + queue_directory)
    print('output_directory=' + output_directory)
    print('concurrent_connections=' + concurrent_connections)
    queue.daemon(queue_type, queue_directory, output_directory, concurrent_connections)


@queue_group.command("fetch")
@click.argument('QUEUE_TYPE')
@click.argument('QUEUE_DIRECTORY')
def queue_fetch(queue_type, queue_directory):
    app_stats, len_all_url_ids = queue.fetch(queue_type, queue_directory)
    print('queue type = ' + queue_type)
    print('number of urls per app / collection\n')
    for app_name, collection_stats in app_stats.items():
        print(app_name + ':')
        for collection_name, num_rows in collection_stats.items():
            print('  ' + collection_name + ': ' + str(num_rows))
    print("\ntotal number of urls in main queue: " + str(len_all_url_ids))


@queue_group.command('download')
@click.argument('QUEUE_TYPE')
@click.argument('QUEUE_DIRECTORY')
@click.argument('OUTPUT_DIRECTORY')
@click.argument('CONCURRENT_CONNECTIONS')
def queue_download(queue_type, queue_directory, output_directory, concurrent_connections):
    # (num_already_downloaded, num_processed, reached_max_downloads, total_read_lines, skipped_due_to_domain_start_time,
    #  num_existing_hash_id, num_new_hash_id, num_error_urls, num_timeout_urls) = \
    queue.download(queue_type, queue_directory, output_directory, concurrent_connections)
    # print('number of already downloaded urls: ' + str(num_already_downloaded))
    # print('number of processed urls: ' + str(num_processed))
    # print('reached max downloads? ' + ('yes' if reached_max_downloads else 'no'))
    # print('total lines read from queue: ' + str(total_read_lines))
    # print('urls skipped due to domain last start time: ' + str(skipped_due_to_domain_start_time))
    # print('urls with existing hashes: ' + str(num_existing_hash_id))
    # print('urls with new hashes: ' + str(num_new_hash_id))
    # print('urls with error: ' + str(num_error_urls))
    # print('urls with timeout: ' + str(num_timeout_urls))


from . import user


@main.group('user')
def user_group():
    pass


@user_group.command('create')
@click.argument("USERNAME")
@click.argument("PASSWORD")
def user_create(username, password):
    user.create(username, password)
    print("Successfully created user " + username)


@user_group.command("change-password")
@click.argument("USERNAME")
@click.argument("PASSWORD")
def user_change_password(username, password):
    user.change_password(username, password)
    print("Successfully changed password for user " + username)


@user_group.command("list")
def user_list():
    _yaml_dump(user.list_users())


@user_group.command("set-superuser")
@click.argument("USERNAME")
@click.option("--remove", is_flag=True)
def user_set_superuser(username, remove):
    user.set_superuser(username, remove)
    if remove:
        print("Successfully removed superuser status form user " + username)
    else:
        print("Successfully set superuser status for user " + username)


@user_group.command("allow-app")
@click.argument("USERNAME")
@click.argument("APP_NAME")
@click.option("--remove", is_flag=True)
def user_allow_app(username, app_name, remove):
    user.allow_app(username, app_name, remove)
    if remove:
        print("Successfully disallowed username %s to access app %s" % (username, app_name))
    else:
        print("Successfully allowed username %s to access app %s" % (username, app_name))
