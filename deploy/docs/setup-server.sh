#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: sudo $0 --domain DOMAIN --deploy-key PUBLIC_KEY_FILE [--ssh-host HOST] [--web-server caddy|none]" >&2
    exit 2
}

domain=""
key_file=""
ssh_host=""
web_server="caddy"
while [ $# -gt 0 ]; do
    case "$1" in
        --domain) domain="${2:-}"; shift 2 ;;
        --deploy-key) key_file="${2:-}"; shift 2 ;;
        --ssh-host) ssh_host="${2:-}"; shift 2 ;;
        --web-server) web_server="${2:-}"; shift 2 ;;
        *) usage ;;
    esac
done
[ -n "$domain" ] && [ -n "$key_file" ] || usage
ssh_host="${ssh_host:-$domain}"
[ -r "$key_file" ] || { echo "cannot read $key_file" >&2; exit 1; }
case "$web_server" in caddy|none) ;; *) usage ;; esac
[ "$(id -u)" -eq 0 ] || { echo "run as root" >&2; exit 1; }

root=/srv/quorium-docs
user=quorium-docs
here="$(cd "$(dirname "$0")" && pwd)"

apt-get update -q
apt-get install -y -q rsync
if [ "$web_server" = caddy ]; then
    apt-get install -y -q caddy
fi

rrsync="$(command -v rrsync || true)"
if [ -z "$rrsync" ] && [ -f /usr/share/doc/rsync/scripts/rrsync.gz ]; then
    gunzip -c /usr/share/doc/rsync/scripts/rrsync.gz > /usr/local/bin/rrsync
    chmod 755 /usr/local/bin/rrsync
    rrsync=/usr/local/bin/rrsync
fi
[ -n "$rrsync" ] || { echo "rrsync not found" >&2; exit 1; }

if ! id "$user" >/dev/null 2>&1; then
    useradd --system --home-dir "/var/lib/$user" --create-home --shell /bin/sh "$user"
fi
install -d -m 755 -o "$user" -g "$user" "$root"

home="$(getent passwd "$user" | cut -d: -f6)"
install -d -m 700 -o "$user" -g "$user" "$home/.ssh"
key="$(head -n 1 "$key_file")"
printf 'command="%s %s",restrict %s\n' "$rrsync" "$root" "$key" > "$home/.ssh/authorized_keys"
chown "$user:$user" "$home/.ssh/authorized_keys"
chmod 600 "$home/.ssh/authorized_keys"

if [ "$web_server" = caddy ]; then
    install -m 644 "$here/Caddyfile" /etc/caddy/Caddyfile
    install -d /etc/systemd/system/caddy.service.d
    printf '[Service]\nEnvironment=DOCS_DOMAIN=%s\n' "$domain" > /etc/systemd/system/caddy.service.d/docs.conf
    systemctl daemon-reload
    systemctl enable caddy
    systemctl restart caddy
fi

echo "Docs root: $root"
echo "Deploy user: $user"
echo "Host key line for the DOCS_KNOWN_HOSTS secret:"
for f in /etc/ssh/ssh_host_ed25519_key.pub; do
    [ -f "$f" ] && echo "$ssh_host $(cut -d' ' -f1,2 "$f")"
done
