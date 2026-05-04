# Quickstart → Pearl migration

Helpers used by `migrate_to_pearl.py` at the repo root. See that script for usage.

Modules:

- `detect.py` — locate `.operate` stores, classify Mode A (fresh copy) vs Mode B (merge).
- `status.py` — read-only source-of-truth queries: docker container presence, port probes, on-chain `ServiceRegistry.ownerOf`, Safe owners + threshold, and filesystem-ownership probes (`is_root_owned`, `any_root_owned_under`).
- `stop.py` — wrap middleware service stop and double-check known docker containers.
- `transfer.py` — the only `MUST_BUILD` on-chain piece: transfer the ServiceRegistry NFT from the quickstart master Safe to the Pearl master Safe. Service-Safe owner swap reuses `gnosis.swap_owner` directly.
- `filesystem.py` — copy `services/sc-{uuid}/` and the referenced `keys/0x{addr}` files. No path rewriting (computed env vars are re-resolved at deploy time — grep for `STORE_PATH` in `olas-operate-middleware/operate/services/service.py`).
- `prompts.py` — interactive helpers for collisions and yes/no questions.
