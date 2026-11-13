# Echo Server

A self-hosted [Matrix](https://matrix.org/) homeserver deployment based on [Conduit](https://conduit.rs) (Rust).

This repository contains the server source and deployment configuration for the Echo Matrix deployment, with an emphasis on containerized deploys and persistent storage options.

## Current status

**Not fully production-ready yet.** Core deployment artifacts exist, but persistence, lockdown, media storage, and TURN must be validated/configured before relying on it.

- ✅ Root `Dockerfile` + `docker/entrypoint.sh` (Render-friendly: maps `PORT` → `CONDUIT_PORT`, defaults `CONDUIT_ADDRESS=0.0.0.0`; `CONDUIT_CONFIG` optional when unset/empty).
- ✅ Main-DB Turso/libSQL backend hardening in this fork (auto-creates KV tables, `CONDUIT_TURSO_*` env mapping via `SUB_TABLES`).
- ✅ Chunked ciphertext media backend in Turso (no S3): files are split into chunks, encrypted at rest with AES-256-GCM (per-file HKDF keys from `CONDUIT_MEDIA_MASTER_KEY`), stored in the main Turso database with per-chunk SHA-256 hashes retrievable via the `media_chunks` table, and purged by the built-in 30-day accessed-retention policy. Configure with `CONDUIT_MEDIA_BACKEND=turso` (see *Media & calls*).
- ⏳ Persistence (Step 1) and the chunked media backend must be validated on the live deployment.
- ⏳ CI-built/pushed image for Render (recommended; avoid Render’s default Rust build), lockdown, and TURN configuration.

## Deploy (container / Render)

Build the image:

```bash
docker build -t echo-server .
```

Run locally (example):

```bash
docker run --rm -p 6167:6167 \
  -e CONDUIT_SERVER_NAME="your.server.name" \
  -e CONDUIT_DATABASE_BACKEND="turso" \
  -e CONDUIT_TURSO_URL="libsql://<your-db>.turso.io" \
  -e CONDUIT_TURSO_AUTH_TOKEN="<secret>" \
  -e CONDUIT_ALLOW_REGISTRATION="false" \
  -e CONDUIT_ALLOW_FEDERATION="false" \
  echo-server
```

**Render:**
- Prefer deploying a **prebuilt image** (CI → registry → Render). Compiling this Rust project on Render’s build can be slow or OOM.
- Set secrets in Render env vars (never in git): `CONDUIT_SERVER_NAME`, `CONDUIT_TURSO_URL`, `CONDUIT_TURSO_AUTH_TOKEN`, registration/TURN credentials as needed.
- Health check: `GET /` returns `Hello from Conduit!`.

## Configuration

- `CONDUIT_CONFIG` points to a TOML file when you want file-based config; if unset/empty, the server can be configured via `CONDUIT_*` environment variables.
- Important env vars: `CONDUIT_SERVER_NAME` (stable), `CONDUIT_DATABASE_BACKEND`, `CONDUIT_DATABASE_PATH`, `CONDUIT_ADDRESS`/`CONDUIT_PORT` (handled by entrypoint), `CONDUIT_ALLOW_REGISTRATION`, `CONDUIT_ALLOW_FEDERATION`.
- Turso (main DB): `CONDUIT_DATABASE_BACKEND=turso`, `CONDUIT_TURSO_URL`, `CONDUIT_TURSO_AUTH_TOKEN`. Turso stores authoritative DB data remotely (persistent); Conduit still uses in-memory caches for performance.
- Turso (media backend): `CONDUIT_MEDIA_BACKEND=turso` + `CONDUIT_MEDIA_MASTER_KEY` (secret, 64-char hex) + optional `CONDUIT_MEDIA_CHUNK_SIZE` (e.g. `"512 KiB"`; default 512 KiB). Requires the main-DB Turso settings above; see *Media & calls*.
- Reference: `conduit-example.toml`, `docs/configuration.md` (upstream-derived; may not reflect all local changes).
- **Secrets:** do not commit `conduit.toml`, `.env`, Turso tokens, TURN credentials, or registration tokens.

## Media & calls

- **Media (chunked Turso backend, recommended for Render):** set `CONDUIT_MEDIA_BACKEND=turso` and `CONDUIT_MEDIA_MASTER_KEY` (a secret 64-character hex string encoding a 32-byte AES-256 key, e.g. from `openssl rand -hex 32`). Files are split into chunks (default 512 KiB, `CONDUIT_MEDIA_CHUNK_SIZE` to override), each chunk is encrypted with AES-256-GCM under a per-file HKDF key, and the ciphertext chunks plus per-chunk SHA-256 hashes are stored in the `media_chunks` table of the main Turso database — so media survives redeploys on ephemeral container filesystems. Reads re-verify every chunk hash, every chunk's GCM tag, and the reassembled file's SHA-256; chunk hashes can be listed for a file without fetching the chunk contents. The built-in retention machinery (default: media not accessed for 30 days) purges both metadata and chunks automatically. Switching to this backend after using the filesystem backend orphans previously stored files (they are only on the old storage); generate thumbnails again by re-uploading if needed.
- **Media (upstream backends):** the filesystem and S3 backends from upstream config remain available when `CONDUIT_MEDIA_BACKEND` is not set to `turso`.
- **1:1 calls:** Conduit exposes TURN discovery (`/_matrix/client/.../voip/turnServer`) but does **not** bundle a TURN server. Use an external TURN provider (e.g., Metered relay) via `CONDUIT_TURN_URIS`/`CONDUIT_TURN_USERNAME`/`CONDUIT_TURN_PASSWORD` (see `docs/turn.md`). Do not self-host coturn on platforms without the required public UDP/port range.

## License

Apache-2.0. See `LICENSE`. This project is based on Conduit and includes upstream components.