FROM quay.io/keboola/docker-base-php56:0.0.2

WORKDIR /home

# Initialize
COPY . /home/
RUN composer install --no-interaction

ENTRYPOINT php ./src/run.php --data=/data
