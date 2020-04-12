CREATE TABLE IF NOT EXISTS "user" (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS superuser (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES "user" (id)
);

CREATE TABLE IF NOT EXISTS app (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS app_user (
  id SERIAL PRIMARY KEY,
  app_id integer NOT NULL,
  user_id integer NOT NULL,
  FOREIGN KEY (app_id) REFERENCES app (id),
  FOREIGN KEY (user_id) REFERENCES "user" (id),
  UNIQUE (app_id, user_id)
);

CREATE TABLE IF NOT EXISTS domain (
  id SERIAL PRIMARY KEY,
  domain TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS url (
  id SERIAL PRIMARY KEY,
  url TEXT UNIQUE NOT NULL,
  domain_id INTEGER NOT NULL,
  FOREIGN KEY (domain_id) REFERENCES domain (id)
);

CREATE TABLE IF NOT EXISTS hash (
  id SERIAL PRIMARY KEY,
  hash TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  download_path TEXT UNIQUE NOT NULL,
  downloaded_at TEXT NOT NULL,
  UNIQUE (hash, size_bytes)
);

CREATE TABLE IF NOT EXISTS url_update_history (
  id SERIAL PRIMARY KEY,
  url_id INTEGER NOT NULL,
  updated_at TEXT NOT NULL,
  hash_id INTEGER,
  error TEXT,
  error_code INTEGER,
  timedout_seconds INTEGER,
  FOREIGN KEY (url_id) REFERENCES url (id)
);

CREATE TABLE IF NOT EXISTS url_last_update (
  id SERIAL PRIMARY KEY,
  url_id INTEGER UNIQUE NOT NULL,
  url_update_history_id INTEGER NOT NULL,
  FOREIGN KEY (url_id) REFERENCES url (id),
  FOREIGN KEY (url_update_history_id) REFERENCES url_update_history (id)
);

CREATE TABLE IF NOT EXISTS url_last_successful_update (
  id SERIAL PRIMARY KEY,
  url_id INTEGER UNIQUE NOT NULL,
  url_update_history_id INTEGER NOT NULL,
  FOREIGN KEY (url_id) REFERENCES url (id),
  FOREIGN KEY (url_update_history_id) REFERENCES url_update_history (id)
);

CREATE TABLE IF NOT EXISTS tag (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS collection (
  id SERIAL PRIMARY KEY,
  app_id integer NOT NULL,
  name TEXT NOT NULL,
  FOREIGN KEY (app_id) REFERENCES app (id),
  UNIQUE (app_id, name)
);

CREATE TABLE IF NOT EXISTS collection_url (
  id SERIAL PRIMARY KEY,
  collection_id integer NOT NULL,
  url_id integer NOT NULL,
  title TEXT,
  metadata TEXT,
  update_freq_minutes integer,
  FOREIGN KEY (collection_id) REFERENCES collection (id),
  FOREIGN KEY (url_id) REFERENCES url (id),
  UNIQUE (collection_id, url_id),
  UNIQUE (collection_id, title)
);

CREATE TABLE IF NOT EXISTS url_tag (
  id SERIAL PRIMARY KEY,
  collection_url_id integer NOT NULL,
  tag_id integer NOT NULL,
  value TEXT NOT NULL,
  FOREIGN KEY (tag_id) REFERENCES tag (id),
  FOREIGN KEY (collection_url_id) REFERENCES collection_url (id),
  UNIQUE (tag_id, collection_url_id)
);

CREATE TABLE IF NOT EXISTS queue (
  url_id INTEGER UNIQUE NOT NULL,
  timeout_seconds INTEGER NOT NULL,
  added_at TEXT NOT NULL,
  status TEXT NOT NULL
);
