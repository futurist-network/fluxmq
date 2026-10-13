# Security policy

## Reporting a vulnerability

Please do not open a public issue. Report it privately through GitHub:
**Security → Report a vulnerability** on <https://github.com/futurist-network/fluxmq/security>.

Include the FluxMQ version (`fluxmq --version`), how it is deployed and, if you can, steps to
reproduce. We confirm receipt within 3 working days, agree on a fix and a disclosure date with you,
and credit you in the release notes unless you prefer otherwise.

## Supported versions

Fixes go into the latest release. FluxMQ is pre-1.0, so upgrade to the newest version to get them.

## Hardening checklist

- Set an admin token (`--admin-token-file`; `deploy/install.sh` generates one). Without any token the
  broker runs in open mode: everyone who reaches its ports is an admin.
- Enable TLS (`--tls-cert`/`--tls-key`, and `--cluster-tls-*` between nodes) when traffic leaves the host.
- Expose only the ports clients need: 7420 (clients) and 7421 (API and console); keep 7422/7423
  (cluster) on a private network.
