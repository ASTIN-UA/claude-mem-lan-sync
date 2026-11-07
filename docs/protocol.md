# The claude-mem sync protocol

Reverse-engineered from claude-mem **13.15.0** and verified end to end against a real worker, then
re-derived from the claude-mem **15.11.1** client source (`src/services/sync/CloudSync.ts`,
`SyncClient.ts`, `CanonicalContent.ts`, `SyncConflictRecovery.ts`, `sync-capabilities.ts`) for the
additions marked **15.x**. This is not a published specification; every claim here was checked
against the client's own validators, and the ones marked **verified live** were additionally confirmed
by driving a real worker against a hub.

## Endpoints

```
GET  /v1/sync/status
POST /v1/sync/ops
GET  /v1/sync/changes?since=<seq>&limit=<n>
GET  /v1/sync/ws                              (13.x only; not served — see X-Sync-Mode)
POST /v1/sync/realtime-token                  (15.x live updates)
GET  /realtime/v1/websocket                   (15.x live updates; Phoenix vsn 1.0.0)
POST /v1/sync/repair/head                     (15.x conflict repair, origin-repair-v1)
POST /v1/sync/repair                          (15.x conflict repair, origin-repair-v1)
```

Every request carries:

```
Authorization: Bearer <token>
X-User-Id: <user id>
X-Device-Id: <uuid the client mints>
X-Device-Name: <display name>          (falls back to the hostname, so effectively always present)
X-Sync-Capabilities: quarantine-unknown-kinds      (15.x; claude-mem 13.29.0 and later)
```

**15.x — `X-Sync-Capabilities`.** Sent on every hub request (status, push, pull, realtime-token).
`quarantine-unknown-kinds` means a pulled op of a kind this build does not know is set aside in
`sync_pull_quarantine` and the cursor moves past it. A build without the header (13.17.2, for one)
validates the kind inside the page decoder (`kind must be observation, summary, or prompt`), throws,
and retries that page forever. The hub stores the header per device (`devices.capabilities`, schema
v2) on every non-probe request; an absent header is stored as NULL, so downgrading a machine closes
the gate below again.

**Verified live**: the exact header set above, `limit=500` on every pull, and `since=0` on a first
sync.

## Constants

| | |
|---|---|
| `protocol_version` | `2` — a JSON number, checked on `/status` and `/changes` |
| `body_schema_version` / `payload_schema_version` | `1` / `2` |
| Max op body | 256,000 UTF-8 bytes |
| Max push request | 4,000,000 bytes |
| Ops per push | 200 in practice (one drain batch); client cap 500 |
| Pull page | always 500, up to 40 pages per cycle |
| Content kinds | `observation`, `summary`, `prompt` |
| Fourth kind | `mutation` — `set_title`, `set_prompt_session`, `remap_project` |
| Decimal strings | `^(?:0|[1-9][0-9]*)$`, ≤ `18446744073709551615` |
| Digest | base64url SHA-256, 43 chars, unpadded |
| `origin_device_id` | ≤128 bytes |
| Poll cadence | 30s active, 300s idle, suspends after 1h |
| Debounce | 1500ms, or 250ms while a WebSocket is live |

**Every sequence, revision, epoch and `server_ts` is a decimal string on the wire, never a JSON
number.**

## Operations

An operation wrapper has exactly two keys:

```json
{ "body": "<canonical JSON string>", "operation_sha256": "<base64url digest of body>" }
```

`body` decodes to exactly twelve keys, sorted:

```
body_schema_version, deleted, deleted_at, entity_rev, id, kind, mutation,
origin_device_id, origin_local_id, payload, payload_schema_version, payload_sha256
```

A real observation push, captured live:

```json
{"protocol_version":2,"ops":[{
  "body":"{\"body_schema_version\":1,\"deleted\":false,\"deleted_at\":null,\"entity_rev\":\"1\",\"id\":\"observation:Xnk4YXtYbz0Jr8TQZLoAlwVUpABoXokKpQhqtZm4B7s\",\"kind\":\"observation\",\"mutation\":null,\"origin_device_id\":\"94a3962b-…\",\"origin_local_id\":\"1\",\"payload\":{…},\"payload_schema_version\":2,\"payload_sha256\":\"dugjMFCJBg4sBYFK2NgbmYOeUZ9vg5tGk27UB-7X4rc\"}",
  "operation_sha256":"ViM6Gst3CgkMwkrE569lCy-NIyU4zUW_2mRbOUuz0-M"}]}
```

Note that **payload integers are decimal strings too** (`created_at_epoch`, `discovery_tokens`,
`prompt_number`). The hub never inspects payloads, but a fixture generator must reproduce this.

### Canonical form

Sorted keys, plain objects, finite numbers, safe integers only, uint64 values as decimal strings.
The client re-canonicalises every body it receives and compares against the raw string, so **a hub
must return body bytes verbatim**.

### Entity ids

```
content:  <kind>:base64url_sha256(JSON.stringify(
              ["cmem-doc-id-v1","device",kind,origin_device_id,origin_local_id]))
mutation: mutation:<uuid>        lowercase, version [1-8], variant [89ab]
```

A hub need not verify this derivation, but it explains why device-id stability matters: change the
device id and every entity gets a new identity.

### Lone surrogates

`JSON.stringify` emits unpaired surrogates as `\udXXX`, and Node's UTF-8 encoder replaces them with
U+FFFD when hashing. So the client's own digest is computed over the replaced bytes:

```
JSON.stringify("a\ud800b")       →  "a\ud800b"
Buffer.from(…, "utf8")           →  61 ef bf bd 62
```

A hub must therefore replace rather than reject, or it will 400 a body the client considers valid.

## `GET /v1/sync/status`

```json
{"protocol_version":2,"epoch":"1","head_seq":"402","projected_seq":"402",
 "content_kinds":["observation","summary","prompt","work_state"]}
```

`epoch` must be a **positive decimal string** within uint64 — a UUID is rejected outright.

**15.x — `content_kinds`.** `CloudSync.runHubStatusProbe` keeps the string entries of an array and
treats a missing or non-array field as `[]` ("a hub that predates `content_kinds` accepts only the
launch kinds"). `hubAcceptsKind` always allows the three launch kinds and allows `work_state` only
when it is listed; rows of an unlisted kind wait unsynced (`heldUnsupportedKind` in the worker's
`/api/sync/status`) and the client re-reads the status hourly while any wait.

The hub lists `work_state` the way the hosted hub does: once **every active device** declared
`quarantine-unknown-kinds`. Active means not revoked and seen within `--active-device-days` (30). No
active device at all keeps it unlisted. `serve --work-state on|off` overrides the rule. A request
carrying `X-Cmemlan-Probe: 1` (cmemlan's own `doctor` and `status`) is answered normally but never
recorded, so diagnostics cannot close the gate.

## `work_state` operations (15.x)

claude-mem's to-do lists and the shared "My Work" records (`work_state_entries`). Same twelve-key
envelope and digests as every content kind (`ENVELOPE_KEYS` is unchanged from 13.x). Specifics:

- `id` is `stableDocumentId("work_state", origin_device_id, origin_local_id)`:
  `"work_state:" + sha256_base64url(canonicalJson(["cmem-doc-id-v1","device","work_state",<device>,<local id>]))`.
- `origin_local_id` is the decimal row id; `entity_rev` the row's `sync_rev`.
- `payload` has exactly `created_at`, `created_at_epoch` (decimal string), `fields` (an object whose
  values are strings, finite numbers, booleans or null), `list_name`, `project`.
- Rows whose `list_name` matches `__my_work_v<N>:*` are pushed only when the sending machine has
  `CLAUDE_MEM_WORK_VIEW_SHARED_STATE=true`; other lists always sync once the hub lists the kind.

The hub accepts `work_state` in `/v1/sync/ops` unconditionally, gate open or not: a push already in
flight when the gate closes must land, because a 400 parks the op at the head of the outbox forever.

## `POST /v1/sync/ops`

```json
{"acked":[{"id":"observation:…","kind":"observation","entity_rev":"1",
           "operation_sha256":"…","seq":"403","origin_local_id":"91"}],
 "head_seq":"403","projected_seq":"403"}
```

**`projected_seq` must equal `head_seq` exactly.** `/status` rejects `projected_seq > head_seq` and
`/ops` rejects `head_seq > projected_seq`; equality is the only value satisfying both. Every acked
`seq` must also be ≤ both.

The acknowledgement rules, each of which throws on the client if broken:

1. Every received op is acked exactly once — multiplicity is per op, not per entity.
2. The ack echoes **the client's** `operation_sha256`, never one read from storage.
3. A duplicate appearing twice in one push is acked twice with the same `seq`.
4. Two distinct tuples must not share a `seq`, and one tuple must not carry two.
5. All six fields are mandatory. `origin_local_id` may be `null` but must be **present** — an omitted
   key is `undefined` and fails the type check.
6. Order is irrelevant.

A duplicate of a stored op consumes no sequence and is re-acked with the original, which makes a
retry after a dropped response free.

## `GET /v1/sync/changes`

```json
{"protocol_version":2,"epoch":"1","head_seq":"403","more":false,
 "ops":[{"seq":"403","server_ts":"1755300000000","body":"…","operation_sha256":"…"}]}
```

`server_ts` is a **decimal string or absent**; a JSON number throws, and so does `null`.

**The client applies with `requireContiguous`, anchored to its stored cursor.** The first op of every
page must be exactly `cursor + 1`. A gap throws, the page rolls back, the cursor does not advance,
and that page is retried forever with backoff to 10 minutes. Consequences for a hub:

- **Never filter out the requesting device's own ops.** The client discards them itself, *after* the
  contiguity check.
- **Never prune from the middle of the log.** Reclaiming space is only legal alongside an epoch bump.
- **Any sequence discontinuity — including restoring from an older backup — requires a new epoch.**
  Otherwise clients sit above `head_seq` polling empty pages forever, with no error.
- The first op is `seq "1"`.

## Errors

There is **no dead-letter path for HTTP failures**. Every non-2xx is one generic throw plus backoff,
and the rejected op stays at the head of the outbox blocking everything behind it. A bad token is
indistinguishable from an outage: the client retries silently, forever.

Therefore:

- Validate minimally. Reject only what cannot be stored or routed.
- Never return 3xx — `fetch` cannot follow it.
- Never return 204 or 205 — they pass `res.ok` and then fail JSON parsing.
- Every success is `200` with a JSON body.
- Reject reasons come from a fixed enum and never echo request bytes; the client copies the first 200
  bytes of an error body into its own log on another machine.

### Deliberate deviation

`limit` outside `[1,500]` is **clamped rather than rejected**. The client always sends 500, and
rejecting a value it will simply repeat would stall its pulls for no benefit.

## `X-Sync-Mode`

`X-Sync-Mode: poll` is aimed at **13.x clients**: it means "do not attempt the `/v1/sync/ws`
WebSocket", so they close any socket and suppress reconnects. Any other value, including an absent
header, re-enables it, and those clients retry a 404 once a minute forever — so the hub keeps sending
`poll` on every response. claude-mem **15.x ignores the header** (SyncClient.ts: "deliberately
ignored here: it must never disable Realtime") and uses the live channel below instead.

**Verified live (13.15.0):** the client logs *"Hub is in poll mode (X-Sync-Mode: poll) — socket
closed, reconnects suppressed, HTTP sync continues"*.

## Live updates (15.x)

Read from `SyncClient.ts` (`fetchRealtimeGrant`, `handleSocketOpen`, `handleSocketMessage`,
`handleBroadcast`, `refreshRealtimeToken`). The hosted hub uses Supabase Realtime; the client speaks
its Phoenix protocol (vsn 1.0.0) with no npm client, so this hub implements just that surface. The
channel is **advisory**: it carries only "the log grew", and the client answers with its ordinary
HTTP pull. Nothing durable rides it, and a lost frame costs at most one poll interval.

Enabled when the client has `CLAUDE_MEM_CLOUD_SYNC_WS` unset or anything but `false`.

### `POST /v1/sync/realtime-token`

Authenticated like every protocol route; no body. Response (all five fields required, `expires_at`
a JSON **number** of unix seconds, `realtime_url` `ws://` or `wss://`, `topic` without a prefix):

```json
{"access_token":"<opaque>","expires_at":1794067200,"realtime_url":"ws://192.168.1.10:8787/realtime/v1/websocket",
 "apikey":"cmemlan","topic":"user:<user id>"}
```

The client appends `apikey` and `vsn=1.0.0` to the URL and joins `realtime:<topic>`. 404 means
"no Realtime here": it stays on polling and re-probes hourly; 401/403 enter its auth pause. It
re-mints at ~80 % of the lifetime (tokens here live an hour). The token is an HMAC over
(user, device, expiry) keyed from the pre-shared key, so rotating the key invalidates every token.

### Socket

`new WebSocket(url)`: **no auth headers**, so the upgrade is unauthenticated and the channel join is
the check. Text frames, JSON objects `{topic, event, payload, ref, join_ref}`.

| client sends | hub answers |
|---|---|
| `phx_join` on `realtime:user:<id>`, `payload.access_token` | `phx_reply` `{status:"ok",response:{}}` with the same `ref`/`join_ref`; `status:"error"` for a bad, expired or revoked token or another user's topic |
| `heartbeat` on topic `phoenix`, every ≤ 25 s | `phx_reply` `{status:"ok"}` with the same `ref` (an unanswered beat makes the client reconnect) |
| `access_token` with a fresh token | `phx_reply` ok; a token for another device ends the socket |
| `phx_leave` | `phx_reply` ok |

The hub sends, after every push and every accepted repair, to joined sockets of **other** devices:

```json
{"topic":"realtime:user:<id>","event":"broadcast","ref":null,
 "payload":{"type":"broadcast","event":"advance","payload":{"epoch":"<epoch>","head_seq":"<head>"}}}
```

Both values are decimal strings. A different epoch makes the client drop the socket and pull over
HTTP (which resets its cursor). A token that expired without a refresh is answered on the next
heartbeat with `{"event":"system","payload":{"status":"error","message":"Token has expired"}}` and
the socket is closed; the client reconnects with fresh credentials. On join the client always runs
one catch-up pull, so advances missed while disconnected cost nothing.

Sockets do not use the request concurrency slots; they have their own cap (64). The server's
read/write timeouts are replaced by a 75 s read deadline (refreshed by each frame) and a 10 s
per-write deadline. A socket too slow to take an advance is dropped.

## Conflict repair, `origin-repair-v1` (15.x)

Read from `SyncConflictRecovery.ts` and `docs/development/viewer-sync-recovery.md`. A device that
owns an entity (the op's `origin_device_id` equals its `X-Device-Id`) can append a strictly newer
revision of its own native row, conditioned on the hub's head and epoch it reviewed. Requests carry
`Authorization`, `X-User-Id`, `X-Device-Id` and `Content-Type: application/json` only — **no**
`X-Sync-Capabilities` and no device name — so the hub does not record the device on these routes
(recording them would close the work_state gate). Any non-200 aborts that one repair; 404 means
`repair_capability_unavailable`.

### `POST /v1/sync/repair/head`

Request `{"entity_id":"<id>"}`. Response:

```json
{"protocol_version":1,"capability":"origin-repair-v1","epoch":"…","head_seq":"…","projected_seq":"…",
 "entity_id":"<id>","head":{"entity_rev":"…","operation_sha256":"…","seq":"…"},"op":{"body":"…","operation_sha256":"…"}}
```

`head` and `op` are both `null` when the log has no revision of the entity. Otherwise they describe
the entity's **highest `entity_rev`** (not the latest seq), and `op` is the stored wrapper with the
body literal byte for byte, as on `/v1/sync/changes`. The client checks that the op's id, origin
and revision match `head`.

### `POST /v1/sync/repair`

Request `{"repair_id":"<32 hex>","expected_epoch":"…","expected_head":null|{entity_rev,operation_sha256,seq},"op":{body,operation_sha256}}`.
The op is validated exactly like a pushed one. In one write transaction the hub:

1. answers an exact retry (same `repair_id`, same entity, revision and digest) from its receipt with
   the original ack, even though the head has since become that op; a reused id with a different op
   is 409;
2. refuses with 403 an op whose origin is not the requesting device;
3. refuses with 409 a changed epoch, a head that is not exactly `expected_head` (including
   "expected none, one exists"), or a revision not strictly above the head's;
4. appends the op at the next seq, records the receipt (`repairs`, schema v2), and announces the
   new head on the live channel.

Response:

```json
{"protocol_version":1,"capability":"origin-repair-v1","epoch":"…","acked":[{"id":"…","kind":"…",
 "entity_rev":"…","operation_sha256":"…","seq":"…","origin_local_id":"…"}],"head_seq":"…","projected_seq":"…"}
```

The client requires exactly one ack matching its op and `seq ≤ head_seq = projected_seq`. Nothing is
ever rewritten: the conflicting revisions stay in the log, and the repair is simply the newest one.
Mutations cannot be repaired (400).

## Client-side mechanics worth knowing

- The push queue is `WHERE synced_at IS NULL AND origin_device_id IS NULL ORDER BY id LIMIT 200`.
  Rows that arrived from the hub keep their origin set, which is what prevents echo loops.
- Migration **v47** recorded every pre-existing row in `sync_launch_exclusions` and stamped
  `synced_at`, so memories that predate cloud sync never upload. See [backfill](backfill.md).
- `sync_launch_exclusions` is consulted on epoch change, not by the flusher.
- The client mints `CLAUDE_MEM_CLOUD_SYNC_DEVICE_ID` and writes it into
  `<dataDir>/settings.json` itself.
