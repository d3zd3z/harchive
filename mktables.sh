#! /bin/sh

# Generate the tables in directory '$1'.

rm -f $1/index.db

sqlite3 $1/index.db <<ZZZZ
pragma page_size = 8192;
-- The file index keeps track of everything stored in the pool.
-- represented by it's rowid (index) as well as its offset into the file.
-- Each field has a kind field.
-- The 'node', and 'offset' fields indicate where this object is stored.
begin transaction;
create table blobs (
  id integer primary key,
  hash blob,
  kind text,
  node numeric,
  offset numeric);
create index blobs_hash_idx on blobs (hash);

-- This takes quite a bit of space and might slow down the backup to
-- store something that will be very infrequently used.
-- TODO: Make a separate table for this information.
create index blobs_kind_idx on blobs (kind);

-- The cache stores information about each directory the last time it was
-- seen.  It lists all of it's children, however that information is merely
-- stored into a blob.
create table cache (
  dev numeric,
  ino numeric,
  info blob not null,
  primary key (dev, ino));

-- As each device is visited, it's UUID is stored in this table to
-- keep a shorter index that will be used for the dev field that will
-- be unique across all backups.
create table devmap (
  dev integer primary key,
  uuid text);
create index devmap_uuid_idx on devmap (uuid);

-- Each pool file is stored here.  Node correlates with the 'node'
-- field in the 'blobs' table.
create table poolfiles (
  node integer primary key,
  path text);

-- Various simple settings.
create table settings (
  name text unique primary key,
  value text);

commit;
ZZZZ
