#!/bin/sh
# Ensure vtdataroot is writable by the vitess user when a Docker volume
# is mounted over it (Docker creates named volumes as root).
if [ "$(id -u)" = "0" ]; then
    chown vitess:vitess /vt/vtdataroot
    exec setpriv --reuid=vitess --regid=vitess --init-groups "$@"
fi
exec "$@"
