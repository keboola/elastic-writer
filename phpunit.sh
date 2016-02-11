#!/usr/bin/env bash


echo "ES HOST $EX_ES_HOST:$EX_ES_HOST_PORT"
curl $EX_ES_HOST:$EX_ES_HOST_PORT/_cluster/health?pretty

env

curl elasticsearch13:9200/_cluster/health?pretty
curl elasticsearch22:9200/_cluster/health?pretty
