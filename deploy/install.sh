#!/bin/sh
# Puts httppooler on this machine: the binary, a system user to run it as, a conf with a fresh secret,
# and the service. Run it again after a rebuild and it upgrades: new binary, same conf, service
# restarted. It never touches a conf that is already there.
set -eu

binary=/usr/local/bin/httppooler
confdir=/etc/httppooler
conf=$confdir/httppooler.conf
unit=/etc/systemd/system/httppooler.service
user=httppooler

here=$(dirname "$0")

# Writing to /usr/local/bin, /etc and the unit directory is root's to do.
if [ "$(id -u)" -ne 0 ]; then
	echo "install.sh: run this as root: sudo ./install.sh" >&2
	exit 1
fi

# The service is a systemd service, and there is nowhere to put it otherwise.
if ! command -v systemctl >/dev/null 2>&1; then
	echo "install.sh: this machine does not run systemd" >&2
	exit 1
fi

# The binary and the unit travel in the same tarball as this script.
for file in httppooler httppooler.service; do
	if [ ! -f "$here/$file" ]; then
		echo "install.sh: $file is not next to this script" >&2
		exit 1
	fi
done

# A binary built for another kind of machine says so here, in one line, instead of later as a service
# that will not start for reasons the log does not explain.
if ! "$here/httppooler" genkey >/dev/null 2>&1; then
	echo "install.sh: the binary does not run on this machine; is it built for this architecture?" >&2
	exit 1
fi

# The service user owns nothing and logs in nowhere. It exists so that the daemon is not root.
if ! id "$user" >/dev/null 2>&1; then
	useradd --system --no-create-home --shell /usr/sbin/nologin "$user"
	echo "Created system user: $user"
fi

# A running binary cannot be written to, but the name pointing at it can be replaced, so the new one is
# put beside it and moved into place.
install -m 0755 -o root -g root "$here/httppooler" "$binary.new"
mv "$binary.new" "$binary"
install -d -m 0750 -o root -g "$user" "$confdir"

fresh=no
if [ ! -e "$conf" ]; then
	fresh=yes
	secret=$("$binary" genkey)
	umask 077
	cat > "$conf" <<CONF
[peer]
#role = server              # or client
#listen = 0.0.0.0:7420      # server only: where clients connect
#server = example.com:7420  # client only: the server to dial
#name = homebox             # for logs; default hostname
#queue_timeout = 10m        # server only: max wait for a free provider, then 503

# A secret for authentication and encryption. This must be the same on the server and on every client.
secret = $secret

#[consume "myservice"]      # applications send their requests here
#listen = 127.0.0.1:8080

#[provide "myservice"]      # a local upstream joins the pool for that service
#upstream = 127.0.0.1:9000
#max_concurrent = 1         # requests run at the same time; match what the upstream really runs
#priority = 100             # smallest number wins when several providers are free
CONF
	chown "root:$user" "$conf"
	chmod 0640 "$conf"
fi

install -m 0644 -o root -g root "$here/httppooler.service" "$unit"
systemctl daemon-reload
systemctl enable httppooler >/dev/null 2>&1
systemctl restart httppooler

echo "HttpPooler installed!"

if [ "$fresh" = yes ]; then
	cat <<NEXT

How to config:

  sudoedit $conf
  sudo systemctl reload httppooler
NEXT
fi
