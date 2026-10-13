#!/bin/sh
# FluxMQ container entrypoint.
#
# * Empty FLUXMQ_* variables are unset, so compose files can pass optional settings as
#   `FLUXMQ_X: ${SOMETHING:-}` without an empty value reaching clap.
# * FLUXMQ_CLUSTER_SECRET_FILE: read the cluster secret from a file (e.g. written by the
#   certs init container into a volume), so no secret lives in a compose file.
# * FLUXMQ_TLS_DIR: a directory written by `fluxmq tls gen-dev` (ca.pem, server.pem,
#   node-<id>.pem, client.pem ...). Unless FLUXMQ_TLS=off it wires
#     broker:  --tls-cert/--tls-key (client port + HTTPS), cluster mTLS when --peers is set,
#              --tls-client-ca when FLUXMQ_CLIENT_MTLS=on;
#     fluxmq-bench / fluxmq-demo: the client CA (+ client certificate when FLUXMQ_CLIENT_MTLS=on).
#   Explicitly set FLUXMQ_TLS_* / FLUXMQ_CLUSTER_TLS_* / FLUXMQ_CLIENT_TLS_* variables win.
# * Admin token: without FLUXMQ_ADMIN_TOKEN / FLUXMQ_ADMIN_TOKEN_FILE / --admin-token* the
#   broker would start in open mode (everyone is admin) on 0.0.0.0. Instead a random token is
#   generated once into $FLUXMQ_DATA_DIR/admin-token and used. FLUXMQ_AUTH=open opts out
#   (local evaluation stacks such as deploy/docker-compose.yml, which bind to 127.0.0.1).
set -eu

for var in $(env | sed -n 's/^\(FLUXMQ_[A-Z0-9_]*\)=$/\1/p'); do
  unset "$var"
done

if [ -n "${FLUXMQ_CLUSTER_SECRET_FILE:-}" ] && [ -z "${FLUXMQ_CLUSTER_SECRET:-}" ]; then
  i=0
  while [ ! -s "$FLUXMQ_CLUSTER_SECRET_FILE" ] && [ $i -lt 60 ]; do sleep 1; i=$((i + 1)); done
  FLUXMQ_CLUSTER_SECRET="$(cat "$FLUXMQ_CLUSTER_SECRET_FILE")"
  export FLUXMQ_CLUSTER_SECRET
fi
unset FLUXMQ_CLUSTER_SECRET_FILE

tls_on() { [ -n "${FLUXMQ_TLS_DIR:-}" ] && [ "${FLUXMQ_TLS:-on}" != "off" ]; }
wait_file() {
  i=0
  while [ ! -s "$1" ] && [ $i -lt 60 ]; do sleep 1; i=$((i + 1)); done
  [ -s "$1" ] || { echo "docker-entrypoint: missing $1 (did the certs init container run?)" >&2; exit 1; }
}

cmd="${1:-fluxmq}"
case "$cmd" in
  -*) set -- fluxmq "$@"; cmd=fluxmq ;;
esac

if [ "$cmd" = "fluxmq" ] && { [ $# -le 1 ] || case "${2:-}" in -*) true ;; *) false ;; esac; }; then
  # Broker mode (no subcommand such as `tls`, `backup`, `restore`).
  token_given=
  for a in "$@"; do case "$a" in --admin-token*) token_given=1 ;; esac; done
  if [ "${FLUXMQ_AUTH:-}" != "open" ] && [ -z "$token_given" ] \
    && [ -z "${FLUXMQ_ADMIN_TOKEN:-}" ] && [ -z "${FLUXMQ_ADMIN_TOKEN_FILE:-}" ]; then
    f="${FLUXMQ_DATA_DIR:-/var/lib/fluxmq}/admin-token"
    if [ ! -s "$f" ]; then
      (umask 077 && tr -dc 'A-Za-z0-9' </dev/urandom | head -c 43 >"$f")
      echo "docker-entrypoint: generated an admin token in $f" >&2
    fi
    echo "docker-entrypoint: console login token: docker exec <container> cat $f" >&2
    FLUXMQ_ADMIN_TOKEN_FILE="$f"
    export FLUXMQ_ADMIN_TOKEN_FILE
  fi
  if tls_on; then
    d="$FLUXMQ_TLS_DIR"
    wait_file "$d/server.pem"
    : "${FLUXMQ_TLS_CERT:=$d/server.pem}" "${FLUXMQ_TLS_KEY:=$d/server-key.pem}"
    export FLUXMQ_TLS_CERT FLUXMQ_TLS_KEY
    if [ "${FLUXMQ_CLIENT_MTLS:-off}" = "on" ]; then
      : "${FLUXMQ_TLS_CLIENT_CA:=$d/ca.pem}"
      export FLUXMQ_TLS_CLIENT_CA
    fi
    if [ -n "${FLUXMQ_PEERS:-}" ] && [ -n "${FLUXMQ_NODE_ID:-}" ]; then
      wait_file "$d/node-$FLUXMQ_NODE_ID.pem"
      : "${FLUXMQ_CLUSTER_TLS_CERT:=$d/node-$FLUXMQ_NODE_ID.pem}"
      : "${FLUXMQ_CLUSTER_TLS_KEY:=$d/node-$FLUXMQ_NODE_ID-key.pem}"
      : "${FLUXMQ_CLUSTER_TLS_CA:=$d/ca.pem}"
      export FLUXMQ_CLUSTER_TLS_CERT FLUXMQ_CLUSTER_TLS_KEY FLUXMQ_CLUSTER_TLS_CA
    fi
  fi
elif [ "$cmd" = "fluxmq-bench" ] || [ "$cmd" = "fluxmq-demo" ]; then
  if tls_on; then
    d="$FLUXMQ_TLS_DIR"
    wait_file "$d/ca.pem"
    : "${FLUXMQ_CLIENT_TLS_CA:=$d/ca.pem}"
    export FLUXMQ_CLIENT_TLS_CA
    if [ "${FLUXMQ_CLIENT_MTLS:-off}" = "on" ]; then
      : "${FLUXMQ_CLIENT_TLS_CERT:=$d/client.pem}" "${FLUXMQ_CLIENT_TLS_KEY:=$d/client-key.pem}"
      export FLUXMQ_CLIENT_TLS_CERT FLUXMQ_CLIENT_TLS_KEY
    fi
  fi
fi

unset FLUXMQ_AUTH
exec "$@"
