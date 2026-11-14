#!/bin/sh
# Покрытие по коду сервисов (без помощников для тестов) с порогом.
set -eu
THRESHOLD=${THRESHOLD:-80}
PKGS=$(go list ./internal/... | grep -v /testutil | paste -sd, -)
go test -race -count=1 -coverpkg="$PKGS" -coverprofile=coverage.out ./internal/...
TOTAL=$(go tool cover -func=coverage.out | awk '/^total:/ {sub("%", "", $3); print $3}')
echo "Покрытие: ${TOTAL}% (порог ${THRESHOLD}%)"
awk -v t="$TOTAL" -v min="$THRESHOLD" 'BEGIN { exit !(t >= min) }' || { echo "Покрытие ниже порога"; exit 1; }
