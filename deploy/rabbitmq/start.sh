#!/bin/sh
# Installs the per-stack Erlang cookie (written by the rabbitmq-cookie init service), then
# starts RabbitMQ through the image's own entrypoint (which drops to the rabbitmq user).
set -eu
if [ -s /cookie/.erlang.cookie ]; then
  cp /cookie/.erlang.cookie /var/lib/rabbitmq/.erlang.cookie
  chown rabbitmq:rabbitmq /var/lib/rabbitmq/.erlang.cookie
  chmod 400 /var/lib/rabbitmq/.erlang.cookie
fi
exec docker-entrypoint.sh rabbitmq-server
