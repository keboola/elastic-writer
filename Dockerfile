FROM php:7.0
MAINTAINER Erik Zigo <erik.zigo@keboola.com>

WORKDIR /home

RUN apt-get update && apt-get install unzip git libxml2-dev -y
RUN cd && curl -sS https://getcomposer.org/installer | php && ln -s /root/composer.phar /usr/local/bin/composer

ADD . /home/
ADD docker/php.ini /usr/local/etc/php/php.ini

RUN composer install --no-interaction

RUN curl --location --silent --show-error --fail \
        https://github.com/Barzahlen/waitforservices/releases/download/v0.3/waitforservices \
        > /usr/local/bin/waitforservices && \
    chmod +x /usr/local/bin/waitforservices

ENTRYPOINT php ./src/run.php --data=/data
