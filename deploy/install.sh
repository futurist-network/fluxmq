#!/bin/sh
# Installs or upgrades FluxMQ as a systemd service on Linux (x86_64 / aarch64, glibc 2.36+):
#
#   curl -fsSL https://raw.githubusercontent.com/futurist-network/fluxmq/master/deploy/install.sh | sudo sh
#
# FLUXMQ_VERSION=v0.11.0 pins a release (default: the latest). An upgrade replaces the binary and
# the unit file and keeps /etc/fluxmq and the data in /var/lib/fluxmq.
set -eu

repo="${FLUXMQ_REPO:-futurist-network/fluxmq}"
version="${FLUXMQ_VERSION:-latest}"

[ "$(id -u)" = 0 ] || { echo "install.sh: run as root (sudo sh)" >&2; exit 1; }
command -v systemctl >/dev/null || { echo "install.sh: systemd is required (or use the Docker image)" >&2; exit 1; }
case "$(uname -m)" in
  x86_64 | amd64) arch=x86_64 ;;
  aarch64 | arm64) arch=aarch64 ;;
  *) echo "install.sh: unsupported CPU $(uname -m)" >&2; exit 1 ;;
esac

asset="fluxmq-linux-$arch.tar.gz"
if [ "$version" = latest ]; then
  url="https://github.com/$repo/releases/latest/download/$asset"
else
  url="https://github.com/$repo/releases/download/$version/$asset"
fi
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
echo "Downloading $url"
curl -fsSL "$url" -o "$tmp/$asset"
curl -fsSL "$url.sha256" -o "$tmp/$asset.sha256"
(cd "$tmp" && sha256sum -c "$asset.sha256" >/dev/null) || { echo "install.sh: checksum mismatch" >&2; exit 1; }
tar -xzf "$tmp/$asset" -C "$tmp"
src="$tmp/fluxmq"

id fluxmq >/dev/null 2>&1 ||
  useradd --system --user-group --home-dir /var/lib/fluxmq --shell /usr/sbin/nologin fluxmq
install -d -o fluxmq -g fluxmq -m 0750 /var/lib/fluxmq
install -d -m 0755 /etc/fluxmq
install -m 0755 "$src/fluxmq" /usr/local/bin/fluxmq
[ -f /etc/fluxmq/fluxmq.toml ] || install -m 0644 "$src/fluxmq.toml" /etc/fluxmq/fluxmq.toml
if [ ! -f /etc/fluxmq/admin-token ]; then
  (umask 027 && od -An -N32 -tx1 /dev/urandom | tr -d ' \n' >/etc/fluxmq/admin-token)
  chgrp fluxmq /etc/fluxmq/admin-token
fi
install -m 0644 "$src/fluxmq.service" /etc/systemd/system/fluxmq.service
systemctl daemon-reload
systemctl enable --quiet fluxmq
systemctl restart fluxmq

echo
echo "FluxMQ $(/usr/local/bin/fluxmq --version | cut -d' ' -f2) is running."
ip="$(hostname -I 2>/dev/null | cut -d' ' -f1)"
echo "  console:      http://${ip:-localhost}:7421"
echo "  admin token:  sudo cat /etc/fluxmq/admin-token"
echo "  config:       /etc/fluxmq/fluxmq.toml   (then: sudo systemctl restart fluxmq)"
echo "  logs:         journalctl -u fluxmq -f"
