#!/bin/sh
# Перезапускает kubectl port-forward к сервису и запоминает PID в файле.
# Старый процесс завершается по PID: поиск по имени (pkill -f) находил бы и саму команду, которая его ищет.
# usage: scripts/port-forward.sh <service> <local-port>
set -eu
svc=$1 port=$2 pidfile=/tmp/pf-$1.pid
if [ -f "$pidfile" ]; then kill "$(cat "$pidfile")" 2>/dev/null || true; fi
kubectl -n aqsha port-forward "svc/$svc" "$port:9090" > "/tmp/pf-$svc.log" 2>&1 &
echo $! > "$pidfile"
# ждём, пока порт начнёт принимать соединения
i=0
until nc -z 127.0.0.1 "$port" 2>/dev/null; do
  i=$((i + 1)); [ "$i" -gt 30 ] && { cat "/tmp/pf-$svc.log"; exit 1; }
  sleep 1
done
