# offline-outbox-js

> Durable offline outbox for field apps: techs keep working with zero signal — every tap is a persisted mutation that syncs with backoff + conflict resolution when the network returns.

Framework-free TypeScript. Zero runtime dependencies. Works in browsers, workers, and Node.

## The 60-second pitch

**Problem.** Field techs lose signal in basements, rural sites, dead zones. Without an offline story, their checklist taps either vanish or require a "don't close the app!" prayer.

**What.** A durable outbox: `enqueue()` every mutation instantly (persisted to `localStorage`), then `flush()` pushes them in order with exponential backoff + jitter, per-mutation status, events, and three conflict strategies.

```ts
import { Outbox, LocalStorageAdapter, FetchTransport } from 'offline-outbox-js';

const outbox = new Outbox({
  storage: new LocalStorageAdapter(),          // durable across reloads
  transport: new FetchTransport('/api/sync'),  // your CRM endpoint
});

outbox.enqueue('complete-step', 'job-step', { stepId: 's3', done: true }, 'tech-7');
await outbox.flush(); // retries with backoff, resolves conflicts
```

## Try it (2 commands)

```bash
npm ci && npm run build
npm run demo          # serves on http://localhost:8040
```

Open **http://localhost:8040/demo/** — a field-tech job checklist:

1. Toggle checklist steps while offline (DevTools → Network → offline, or the in-page switch) — the queue badge counts pending mutations.
2. Hit **Sync now** — queued steps flush with simulated latency.
3. Reload mid-queue — the queue survives (localStorage adapter).
4. Arm **Conflict on next sync** — see `keep-both` park a mutation for manual review (retry / drop).

(Use the local server, not `file://` — ES modules need `http`.)

## How it works

```
enqueue() → persist → flush() → transport.send() ─┬─ ack      → status acked
                                                  ├─ conflict → strategy decides
                                                  └─ throw    → backoff + retry (up to maxAttempts → failed)
```

- **FIFO order**, crash recovery (`sending` → `pending` on reload), one poisoned mutation never blocks the rest.
- **Concurrent-safe**: a second `flush()` while one is running is a no-op; acked/failed/conflict entries are never resent.

## API

| Method | Signature | What it does |
|---|---|---|
| `enqueue` | `(op, entity, payload, actor?) → StoredMutation` | Persist + queue a mutation, emits `enqueued` |
| `flush` | `() → Promise<FlushSummary>` | Push pending in FIFO order with retries, emits `drained` when nothing is pending |
| `getQueue` / `getPending` / `getPendingCount` | `() → …` | Inspect the queue (copies, safe to render) |
| `getById` | `(id) → StoredMutation?` | Per-mutation status + attempts + error + resolution |
| `resolveConflict` | `(id, 'retry' \| 'drop') → boolean` | Manual review for `keep-both` conflicts |
| `delayFor` | `(attempt) → ms` | Backoff schedule (pure, testable) |
| `prune` | `() → number` | Drop terminal `acked`/`failed` entries |
| `clear` | `() → void` | Empty the queue |
| `on` | `(event, fn) → unsubscribe` | Subscribe to events |

**Mutation shape:** `{ id, op, entity, payload, ts, actor }` + status fields (`status, attempts, nextRetryAt, error?, resolution?`).

**Statuses:** `pending → sending → acked | failed | conflict`.

### Events

| Event | Payload | When |
|---|---|---|
| `enqueued` | mutation | a mutation is queued |
| `acked` | mutation | server acked it (or a strategy resolved it) |
| `failed` | mutation | retries exhausted (`mutation.error` kept) |
| `conflict` | mutation | parked for manual review (`keep-both`) |
| `drained` | `null` | flush finished with zero pending |

### Storage adapters

| Adapter | Durable? | Default where? |
|---|---|---|
| `LocalStorageAdapter(key?)` | yes (survives reload/crash) | browsers |
| `MemoryStorage` | no | Node / tests |

### Transports (injectable)

| Transport | Use |
|---|---|
| `FetchTransport(url, { fetchFn?, conflictStatuses? })` | production: POSTs JSON, maps 409 → conflict, non-2xx → retryable throw |
| `FakeTransport({ latencyMs?, failFirst?, alwaysFail?, failIds?, conflictIds? })` | tests + demo, zero network |

## Conflict matrix

| Strategy | On conflict | Final status | Needs a human? |
|---|---|---|---|
| `last-write-wins` (default) | newer timestamp wins (no server ts → local wins) | `acked` (`local-newer` / `server-newer`) | no |
| `server-wins` | local edit discarded | `acked` (`server-wins`) | no |
| `keep-both` | parked for review | `conflict` (`manual-review`) | yes — `resolveConflict(id, 'retry' \| 'drop')` |

## Backoff

`delay(attempt) = min(maxMs, baseMs · 2^attempt) ± jitter` — defaults `base 500ms / max 30s / 5 attempts / ±20%`, all configurable. `delayFor()` is pure (inject `random` for deterministic tests); `sleep` and `now` are injectable for fake-timer testing.

## Scripts

```bash
npm ci            # install (devDeps only: typescript + vitest)
npm test          # 36 tests, Vitest
npm run build     # tsc → dist/ (ESM + .d.ts, zero-dep)
npm run demo      # http://localhost:8040/demo/
```

## Project layout

```
src/outbox.ts       Outbox, MemoryStorage, LocalStorageAdapter, backoff, events
src/transports.ts   Transport, FetchTransport, FakeTransport
src/conflicts.ts    last-write-wins / server-wins / keep-both
src/index.ts        public barrel
tests/              outbox.test.ts (29) + transports.test.ts (7)
demo/index.html     single-file field-tech checklist (imports ../dist, no bundler)
```

## Screenshots

`screenshots/demo-1440.png` — the field-tech checklist + outbox drawer, captured live (`npm run demo`, port 8040).

## Honest limits

- **No IndexedDB adapter yet** — `localStorage` caps around 5MB; large photo payloads need IDB (interface is ready, PR-shaped hole).
- **No E2E encryption** — queued payloads sit in plaintext; fine for checklists, not for PII-heavy fields.
- **Capacitor/native shell untested** — web-only so far; the lib is runtime-agnostic but no device testing has been done.
- **LWW needs server timestamps to be exactly right** — clock skew can flip a close call; `keep-both` exists for the cases that matter.
- **No mutation compaction/dedup** — ten toggles of one checkbox sync as ten mutations; call `prune()` to shed terminal entries.

## License

MIT
