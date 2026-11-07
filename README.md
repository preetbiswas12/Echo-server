# Echo Server

A self-hosted Matrix homeserver deployment based on [Conduit](https://conduit.rs) (Rust).

This repository contains the server source/build configuration used for the Echo Matrix deployment, with a focus on reproducible container builds and persistent storage backends (e.g., Turso/libSQL where enabled).

## Status

Work in progress. Deployment hardening (persistence validation, lockdown, reproducible image builds, media storage/cleanup, TURN) should follow the project checklist before production use.

## Quick start (container)

Build and run with the root `Dockerfile`:

```bash
docker build -t echo-server .
docker run --rm -p 6167:6167 \
  -e CONDUIT_SERVER_NAME="your.server.name" \
  -e CONDUIT_DATABASE_PATH="/var/lib/matrix-conduit" \
  -e CONDUIT_DATABASE_BACKEND="sqlite" \
  -e CONDUIT_ALLOW_REGISTRATION="false" \
  -e CONDUIT_ALLOW_FEDERATION="false" \
  echo-server
```

### Render notes
- The container listens on `CONDUIT_ADDRESS=0.0.0.0` and uses `CONDUIT_PORT` (the entrypoint maps Render’s `PORT` to `CONDUIT_PORT`).
- `CONDUIT_CONFIG` is optional: when unset/empty, configuration can be provided via `CONDUIT_*` environment variables. Set `CONDUIT_CONFIG` to a TOML file path if you want file-based config.
- Do not rely on Render’s default Rust build for production; build/push an image (CI) and deploy the image (see `Dockerfile`).

## Configuration

- Config reference: `conduit-example.toml` and `docs/configuration.md` (upstream-derived docs may not reflect all local changes).
- Key env vars: `CONDUIT_SERVER_NAME`, `CONDUIT_DATABASE_BACKEND`, `CONDUIT_DATABASE_PATH`, `CONDUIT_ALLOW_REGISTRATION`, `CONDUIT_ALLOW_FEDERATION`, and (when using Turso) `[global.turso]` / `CONDUIT_TURSO_*` as supported by the current build.
- **Secrets:** never commit `conduit.toml`, `.env`, Turso tokens, TURN credentials, or registration tokens. Use Render/environment secrets.

## Storage
- Main DB backends depend on compiled features and config. Turso/libSQL support exists in this fork; ensure the deployed build includes it and that required tables/schema are available.
- Media backend work (e.g., chunked encrypted storage) may be in progress; verify the active implementation/config before relying on it for production data.

## Calls (1:1 VoIP)
Conduit provides TURN discovery (`/_matrix/client/.../voip/turnServer`) but does **not** bundle a TURN server. Configure `[global.turn]`/TURN env vars with an external TURN provider (e.g., Metered relay) per `docs/turn.md`.

## License

Apache-2.0. See `LICENSE`. This project is based on Conduit and includes upstream components; review source headers and upstream history for full attribution.