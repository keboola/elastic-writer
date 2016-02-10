#!/usr/bin/env bash

composer install -n

echo "ES HOST $EX_ES_HOST:$EX_ES_HOST_PORT"
curl $EX_ES_HOST:$EX_ES_HOST_PORT

env

./vendor/bin/phpunit
