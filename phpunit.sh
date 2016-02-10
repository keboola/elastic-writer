#!/usr/bin/env bash

composer install -n

echo "ES HOST $EX_ES_HOST:$EX_ES_HOST_PORT"
curl $EX_ES_HOST:$EX_ES_HOST_PORT/_nodes?pretty

env

./vendor/bin/phpunit
