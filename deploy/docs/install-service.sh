#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: sudo $0 --port PORT [--host HOST] [--root DIR] [--source REPO_DIR]" >&2
    exit 2
}

host="0.0.0.0"
port=""
root="/srv/quorium-docs"
source_dir=""
while [ $# -gt 0 ]; do
    case "$1" in
        --host) host="${2:-}"; shift 2 ;;
        --port) port="${2:-}"; shift 2 ;;
        --root) root="${2:-}"; shift 2 ;;
        --source) source_dir="${2:-}"; shift 2 ;;
        *) usage ;;
    esac
done
[ -n "$host" ] && [ -n "$port" ] && [ -n "$root" ] || usage
[[ "$port" =~ ^[0-9]+$ ]] && [ "$port" -ge 1 ] && [ "$port" -le 65535 ] || { echo "invalid port: $port" >&2; exit 2; }
[[ "$root" = /* ]] || { echo "--root must be an absolute path" >&2; exit 2; }
[ "$(id -u)" -eq 0 ] || { echo "run as root" >&2; exit 1; }
if [ -n "$source_dir" ]; then
    source_dir="$(cd "$source_dir" && pwd)"
    [ -f "$source_dir/documentation/conf.py" ] || { echo "$source_dir is not a Quorium checkout" >&2; exit 1; }
fi

service=quorium-docs
user=quorium-docs
config_dir=/etc/$service
unit=/etc/systemd/system/$service.service

packages=(caddy)
[ -n "$source_dir" ] && packages+=(make python3-venv rsync)
apt-get update -q
apt-get install -y -q "${packages[@]}"

if systemctl is-enabled --quiet caddy 2>/dev/null && ! dpkg -V caddy 2>/dev/null | grep -q " /etc/caddy/Caddyfile$"; then
    systemctl disable --now caddy
    echo "Disabled the caddy package's default site, which listens on port 80"
fi

if ! id "$user" >/dev/null 2>&1; then
    useradd --system --home-dir "/var/lib/$user" --create-home --shell /usr/sbin/nologin "$user"
fi
install -d -m 755 -o "$user" -g "$user" "$root"

if [ -n "$source_dir" ]; then
    builder="${SUDO_USER:-root}"
    [ "$(stat -c %U "$source_dir")" = "$builder" ] || builder=root
    runuser -u "$builder" -- make -C "$source_dir/documentation" html BUILD=_build/latest
    rsync -r --delete --chmod=D755,F644 "$source_dir/documentation/_build/latest/html/" "$root/latest/"
    chown -R "$user:$user" "$root/latest"
fi

install -d -m 755 "$config_dir"
cat > "$config_dir/Caddyfile" <<EOF
{
	admin off
	auto_https off
}

:$port {
	bind $host
	root * $root
	encode zstd gzip

	header {
		X-Content-Type-Options "nosniff"
		Referrer-Policy "strict-origin-when-cross-origin"
		-Server
	}

	@static path_regexp ^/[^/]+/_static/
	header @static Cache-Control "public, max-age=86400"

	redir / /latest/ 302

	file_server

	handle_errors {
		@missing expression {err.status_code} == 404
		handle @missing {
			rewrite * /latest/404/index.html
			file_server
		}
	}
}
EOF
caddy validate --adapter caddyfile --config "$config_dir/Caddyfile" >/dev/null

cat > "$unit" <<EOF
[Unit]
Description=Quorium documentation website
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
User=$user
Group=$user
ExecStart=$(command -v caddy) run --adapter caddyfile --config $config_dir/Caddyfile
Restart=on-failure
RestartSec=2
StateDirectory=$service
Environment=HOME=/var/lib/$service
Environment=XDG_DATA_HOME=/var/lib/$service
Environment=XDG_CONFIG_HOME=/var/lib/$service
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictNamespaces=true
LockPersonality=true
ReadOnlyPaths=$root

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$service" >/dev/null
systemctl restart "$service"
systemctl is-active --quiet "$service" || { journalctl -u "$service" -n 20 --no-pager >&2; exit 1; }

[ -f "$root/latest/index.html" ] || echo "warning: $root/latest is empty; deploy the site there or rerun with --source" >&2
echo "Serving $root on http://$host:$port/ as systemd service $service"
