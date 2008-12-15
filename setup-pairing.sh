#! /bin/bash

# Configuring the pair with the secrets is current a bit challenging.
# The script takes care of fresh setups.

CLIENTCONF=/tmp/client-config.sqlite3 
POOLCONF=/tmp/pool-config.sqlite3 

POOLNICK=2008-10
POOL=/backup/pool-2008-10

if [ -f ${CLIENTCONF} ]; then
   echo "Client config file already exists: ${CLIENTCONF}"
   exit 1
fi
if [ -f ${POOLCONF} ]; then
   echo "Pool config file already exists: ${POOLCONF}"
   exit 1
fi

./harchive pool setup
./harchive client setup

./harchive pool add ${POOLNICK} ${POOL}
pool_uuid=$(sqlite3 ${POOLCONF} \
   "select uuid from pools where nick = '${POOLNICK}'")

client_uuid=$(sqlite3 ${CLIENTCONF} \
   "select value from config where key='uuid'")

./harchive pool client ${client_uuid}

secret=$(sqlite3 ${POOLCONF} \
   "select secret from clients where uuid='${client_uuid}'")

echo Secret: ${secret}
./harchive client pool ${POOLNICK} ${pool_uuid} localhost 8933 "${secret}"
