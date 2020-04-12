## Quickstart

Start the DB

```
docker run --name downloader-db -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 -v `pwd`/.db:/var/lib/postgresql/data postgres:12
```

Install from source

```
sudo apt-get install libcurl4-openssl-dev
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

Set DB connection

```
export DOWNLOADER_DB_DSN="dbname=postgres user=postgres password=mysecretpassword host=localhost port=5432"
```

In a separate terminal, serve the output directory for development:

```
mkdir -p .output
cd .output && python3 -m http.server
```

In the main terminal, export the http location of the output:

```
export OUTPUT_URL_PREFIX=http://localhost:8000
```

Migrate the DB (safe to run multiple times)

```
downloader db migrate
```

### Create an app

```
downloader app create APP_NAME
```

### List apps

```
downloader app list
```

### Add a URL

```
downloader url add APP_NAME URL [EXTRA_JSON]
```

Example EXTRA_JSON:

```
{
  "title": "URL_TITLE",
  "collection": "COLLECTION_NAME",
  "tags": {
    "my-tag-1": "foo",
    "my-tag-2": "bar"
  },
  "metadata": {...},
  "update_freq_minutes": 60
}
```

All attributes are optional:

* `title` - unique url title within each collection, if not provided, the URL will be used as the title
* `collection` - collection name for retrieval, does not have to be created beforehand
                 if not specified, `default` is used as the collection name
* `tags` - custom string tags, can be used for URL retrieval
* `metadata` - custom metadata object
* `update_freq_minutes` - minutes to re-fetch the URL (not guaranteed)
                          if not set, will never re-fetch the URL (the default)

Possible errors:

* URL or URL title already exists in the collection

Return value: ID

### Edit URL

```
downloader url edit <ID> <UPDATE_JSON>
```

UPDATE_JSON is the same as EXTRA_JSON and can used to change any of the URL's attributes except for `collection`

All attributes will overwrite existing url values (all tags / metadata will be replaced)

Attributes which are not present in the UDPATE_JSON will not be changed.

Possible errors:

* title already exists in collection

### get / search URL

```
downloader url get <ID> [--with-history] [--with-tags]
downloader url search APP_NAME COLLECTION_NAME [--title=] [--title-startswith=] [--title-contains=] [--tag=NAME=VALUE] [--with-history] [--with-tags]
```

COLLECTION_NAME should be `default` if url was created without a collection

only one of the following arguments can be used for a search: `TITLE`, `--title-startswith`, `--title-contains`, `--tag`

get command returns a single url object, search returns a list of up to 100 url objects

example search response:

```
- url: "URL"
  title: "TITLE"
  download_url: "DOWNLOAD_URL"
  hash: "DOWNLOADED_URL_HASH"
  size_bytes: 500
  update_freq_minutes: 0
  last_update: 2012-04-23T18:25:43
  last_update_minutes: 50
  next_update_minutes: 0
  metadata: {...}
  tags: {...}
  history:
  - date: 2012-04-21T18:25:43
    download_url: "DOWNLOAD_URL"
    hash: "DOWNLOADED_URL_HASH"
    size_bytes: 400
  - date: 2012-04-20T18:25:43
    hash: "DOWNLOADED_URL_HASH"
    size_bytes: 300
```

Where:

* `update_freq_minutes`: will be 0 if no update was requested
* `last_update`: standard datetime string in UTC timezone
* `last_update_minutes`: how many minutes since last updates
* `next_update_minutes`: will be 0 if no update was requested
* `tags`: only if --with-tags - returns relted tags (get: 100 tags, search: 10 tags)
* `history`: only if --with-history - returns last updates (get: last 100 updates, search: last 10 updates)

### Get app collections and number of urls per collection

```
downloader app get-collections APP_NAME
```

### Downloader daemon

You should run 3 daemons for the different queue types:

```
downloader queue daemon <QUEUE_TYPE> <QUEUE_DIRECTORY> <OUTPUT_DIRECTORY> <CONCURRENT_CONNECTIONS>
```

* `QUEUE_TYPE`: one of `samedomain` / `timedout` / `regular`
* `QUEUE_DIRECTORY`: different directory should be used for each queue type, directory should not exist beforehand
* `OUTPUT_DIRECTORY`: shared directory between all queue types which contains all the downloaded data
* `CONCURRENT_CONNECTIONS`: number of concurrent connections to use for downloading

For example:

```
downloader queue daemon timedout .queue-timedout .output 5 &
downloader queue daemon samedomain .queue-samedomain .output 5 &
downloader queue daemon regular .queue-regular .output 5 &
```

Following queue commands should be used only for manual debug / development, they are used internally by the downloader daemon

Fetch from DB and store in the queue directory (QUEUE_DIRECTORY must not exist beforehand to prevent race conditions)

```
downloader queue fetch <QUEUE_TYPE> <QUEUE_DIRECTORY>
```

Possible options for QUEUE_TYPE:

* `timedout`: URLs which previously timed out at more then 15 seconds
* `samedomain`: URLs which didn't previously time out and have more then 50 urls in the queue with the same domain
* `regular`: All other URLs

The following URLs are fetched:

* New URLs which were never downloaded before
* URLs whose last update failed and have all the following conditions:
    * less then 5 consecutive failed last updates
    * last failed update was more then 10 minutes ago
* URLs whose last successful update was more then update_freq_minutes ago and are not in the last failed conditions

Download URLs from the queue:

```
downloader queue download <QUEUE_TYPE> <QUEUE_DIRECTORY> <OUTPUT_DIRECTORY> <CONCURRENT_CONNECTIONS>
```

* `QUEUE_TYPE`: one of `samedomain` / `timedout` / `regular`, must match the QUEUE_DIRECTORY used for fetch with the same queue type

### REST API

Start the REST API for development

```
env FLASK_APP=downloader.api flask run
```

REST API is authenticated using HTTP auth

All requests must be authenticated with a superuser or a user with permissions to the requested app

Following URL routes are available with same argument names as the CLI commands but should be passed in the query string

* `/url/add` - `downloader url add`
* `/url/edit` - `downloader url edit`
* `/url/list` - `downloader url list`

Response status_code indicates success or failure

Response body is a json object with the following keys:

* `ok` - boolean indicating success or failure
* `error` - only returned in case of error, contains string with the error message
* additional method specific keys might also be returned

### User management

Create a user:

```
downloader user create <USERNAME> <PASSWORD>
```

Change password:

```
downloader user change-password <USERNAME> <PASSWORD>
```

Allow user access to an app

```
downloader user allow-app <USERNAME> <APP_NAME>
```

Set or remove superuser status (allows access to all apps):

```
downloader user set-superuser <USERNAME> [--remove]
```

List users:

```
downloader user list
```
