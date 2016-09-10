#!/bin/bash

docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag keboola/elastic-writer quay.io/keboola/elastic-writer:$TRAVIS_TAG
docker push quay.io/keboola/elastic-writer:$TRAVIS_TAG
