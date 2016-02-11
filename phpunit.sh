#!/usr/bin/env bash

composer install -n

waitforservices

./vendor/bin/phpunit
