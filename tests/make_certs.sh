#!/usr/bin/env bash
# Self-signed test certificates for the mock platform (localhost / 127.0.0.1).
# server.pem: valid 365 days. short.pem: valid 12 days (exercises the route TLS expiry warning).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p certs
for spec in "server 365" "short 12"; do
  set -- $spec
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days "$2" \
    -subj "/CN=localhost/O=AAP Health Mock" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    -keyout "certs/$1.key" -out "certs/$1.pem" >/dev/null 2>&1
done
echo "certificates written to $(pwd)/certs"
