# edge-bridge

Small WebSocket TCP bridge with health endpoints.

- `GET /` -> `ok`
- `GET /generate_204` -> `204`
- `GET /sub.yaml` -> YAML profile, host taken from the request `Host` header
- `WS /ws` -> binary TCP bridge, 16-byte session key

Listens on `0.0.0.0:$PORT`.
