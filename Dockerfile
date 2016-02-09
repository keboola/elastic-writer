FROM quay.io/keboola/docker-base-php56

WORKDIR /home

# Initialize
COPY . /home/
RUN composer install --no-interaction

ENTRYPOINT php ./src/run.php --data=/data
