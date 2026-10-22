#!/usr/bin/env bash
set -euxo pipefail
# Oracle's URL uses 2326300 for 23.26.3.0.0; 19/21 ZIPs add "dbru".
IFS=. read -r MAJ MIN PATCH _ <<< "$ORACLE_CLIENT"
VERDIR="${MAJ}${MIN}${PATCH}00"
ICDIR="/opt/oracle/instantclient_${MAJ}_${MIN}"
SUFFIX=''
if [ "$MAJ" -le 21 ]; then SUFFIX='dbru'; fi
BASE="https://download.oracle.com/otn_software/linux/instantclient/${VERDIR}"
curl --retry 3 --retry-delay 5 -fsSL -o /tmp/ic-basiclite.zip "$BASE/instantclient-basiclite-linux.x64-${ORACLE_CLIENT}${SUFFIX}.zip"
curl --retry 3 --retry-delay 5 -fsSL -o /tmp/ic-sdk.zip        "$BASE/instantclient-sdk-linux.x64-${ORACLE_CLIENT}${SUFFIX}.zip"
sudo mkdir -p /opt/oracle
# Both ZIPs contain META-INF files; overwrite them without prompting.
sudo unzip -qo /tmp/ic-basiclite.zip -d /opt/oracle
sudo unzip -qo /tmp/ic-sdk.zip -d /opt/oracle
# Backends load libclntsh when they load the extension.
echo "$ICDIR" | sudo tee /etc/ld.so.conf.d/oracle-instantclient.conf >/dev/null
sudo ldconfig
# GITHUB_ENV exposes ORACLE_HOME to subsequent build/install steps.
echo "ORACLE_HOME=$ICDIR" >> "$GITHUB_ENV"
