#!/usr/bin/env bash


echo "ES HOST $EX_ES_HOST:$EX_ES_HOST_PORT"
curl $EX_ES_HOST:$EX_ES_HOST_PORT/_cluster/health?pretty

env

cat /etc/hosts

curl $ELASTICSEARCH13_PORT_9200_TCP_ADDR:$ELASTICSEARCH13_PORT_9200_TCP_PORT
curl $ELASTICSEARCH22_PORT_9200_TCP_ADDR:$ELASTICSEARCH22_PORT_9200_TCP_PORT

curl elasticsearch13:9200/_cluster/health?pretty
curl elasticsearch22:9200/_cluster/health?pretty
