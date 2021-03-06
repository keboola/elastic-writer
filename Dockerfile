FROM php:7.0
MAINTAINER Erik Zigo <erik.zigo@keboola.com>

RUN apt-get update -q \
  && apt-get install ssh unzip git libxml2-dev -y --no-install-recommends

WORKDIR /root

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

COPY docker/php.ini /usr/local/etc/php/php.ini

COPY . /code

WORKDIR /code

RUN composer install --prefer-dist --no-interaction

ENTRYPOINT php ./src/run.php --data=/data
