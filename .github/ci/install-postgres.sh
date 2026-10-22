#!/usr/bin/env bash
set -euxo pipefail
sudo apt-get update -qq
sudo apt-get install -y -qq postgresql-common ca-certificates
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
sudo apt-get install -y -qq \
  postgresql-"$PG_VERSION" \
  postgresql-server-dev-"$PG_VERSION" \
  postgresql-"$PG_VERSION"-postgis-3 \
  libaio1 \
  unzip
echo "/usr/lib/postgresql/$PG_VERSION/bin" >> "$GITHUB_PATH"
