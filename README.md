# gtk-sync

LAN file sync (client + server) for the hyprCore Linux GTK apps. Originally MimicFS; packaged here as **gtk-sync**.

On hyprCore Linux desktops, gtk-files **Setup Sync** launches this tree from
`/usr/local/lib/neuronix/gtk-apps/gtk-sync/` (populated by `syn-to-devices.sh`
with prebuilt `target/release/` binaries so cargo is not required on the ISO).

Clients push/pull over **HTTPS (TLS)** with a shared username/password. Servers keep timestamped history in CouchDB + on-disk blobs (default retention **24 hours**).

## Install

```bash
cd gtk-sync   # from this suite's checkout root
./install.sh
```

Choose **Setup server** or **Setup client**, pick any folder, set password. Server setup (after sudo) will:

1. Build/install `gtk-sync`
2. Install Docker if needed
3. Start CouchDB (`gtk-sync-couchdb` on `127.0.0.1:5984`)
4. Enable systemd unit **`gtk-sync.service`**

Client enables user unit **`gtk-sync-client.service`**.

### Non-interactive

```bash
./install.sh --server --folder /var/lib/gtk-sync --password 'secret' --port 8443
./install.sh --client --folder /path/to/sync --password 'secret' --server-host 192.168.1.10
```

(`--folder` is optional for `--server`; it defaults to `/var/lib/gtk-sync`.)

## Services & paths

| Role | Binary | Config | Storage | Service |
|------|--------|--------|---------|---------|
| Server | `/usr/local/bin/gtk-sync` | `/etc/gtk-sync/server.toml` | `/var/lib/gtk-sync` | `systemctl status gtk-sync` |
| Client | `~/.local/bin/gtk-sync-client` | `~/.config/gtk-sync/client.toml` | user-chosen folder | `systemctl --user status gtk-sync-client` |

Logs:

```bash
journalctl -u gtk-sync -f
journalctl --user -u gtk-sync-client -f
```

Client writes live UI status for gtk-files:

```bash
cat "${XDG_RUNTIME_DIR:-$HOME/.config}/gtk-sync/status.json"
# versions --json for restore dialogs
gtk-sync-client versions --json path/to/file.txt
```

## Uninstall

```bash
./uninstall.sh
```

## Layout

| Path | Role |
|------|------|
| `install.sh` / `uninstall.sh` | Installer |
| `mimic-core/` | Shared library (internal crate name) |
| `host/` | `gtk-sync` server binary + `gtk-sync.service` |
| `client/` | `gtk-sync-client` + `gtk-sync-client.service` |
| `scripts/` | CouchDB helper, smoke test |

Default HTTPS port **8443**.

## License

GNU General Public License v3.0. See [LICENSE](LICENSE).
