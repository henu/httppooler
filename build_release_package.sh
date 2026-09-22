#!/bin/sh
# Runs the tests, builds the static binary, and packs it with the install script and the unit into
# httppooler.tar.gz: the one file to copy to a machine.
set -eu

cd "$(dirname "$0")"

# Staged outside the tree, so nothing here is overwritten and nothing is left behind.
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Every step is quiet until it fails, and then it says everything it had to say.
run() {
	if ! "$@" >"$work/log" 2>&1; then
		cat "$work/log" >&2
		exit 1
	fi
}

echo "Downloading dependencies..."
run go mod download

echo "Testing..."
run go test -buildvcs=false ./...

echo "Building..."
mkdir "$work/httppooler"
run env CGO_ENABLED=0 go build -trimpath -buildvcs=false "-ldflags=-s -w" \
	-o "$work/httppooler/httppooler" ./cmd/httppooler

echo "Packaging..."
cp deploy/install.sh deploy/httppooler.service "$work/httppooler/"
run tar czf httppooler.tar.gz -C "$work" httppooler

echo "Success!"
