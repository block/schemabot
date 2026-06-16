#!/bin/sh
# vtcombo's embedded mysqld writes its data dir, my.cnf, and sockets under
# VTDATAROOT, so that path must be writable by the vitess user the server runs
# as. When a volume (Docker named volume or Kubernetes emptyDir) is mounted over
# it, the mount is created root-owned, so chown it before dropping privileges.
VTDATAROOT="${VTDATAROOT:-/vt/vtdataroot}"
if [ "$(id -u)" = "0" ]; then
    chown vitess:vitess "$VTDATAROOT"
    exec setpriv --reuid=vitess --regid=vitess --init-groups "$@"
fi
exec "$@"
