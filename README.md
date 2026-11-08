# offline-outbox-js — Durable Offline Outbox for Field Apps

> Techs keep working with zero signal — every tap is a persisted mutation that syncs with backoff + conflict resolution when the network returns.

![TypeScript](https://img.shields.io/badge/TypeScript-5-blue) ![Zero deps](https://img.shields.io/badge/runtime_deps-0-brightgreen) ![Tests](https://img.shields.io/badge/tests-36_Vitest-brightgreen) ![License](https://img.shields.io/badge/license-MIT-green)

Framework-free TypeScript. Zero runtime dependencies. Browsers, workers, and Node.

**🎬 Live demo:** `npm run demo` → http://localhost:8040/demo/ (field-tech checklist, no bundler) · **📸 Screenshot:** `screenshots/demo-1440.png`

## The problem it solves

Field techs lose signal in basements, rural sites, dead zones. Without an offline story, checklist taps vanish or demand a "don't close the app!" prayer. This lib is the fix: `enqueue()` every mutation instantly (persisted to `localStorage`), then `flush()` pushes them in FIFO order with exponential backoff + jitter, per-mutation status, events, and three conflict strategies.

```ts
import { Outbox, LocalStorageAdapter, FetchTransport } from 'offline-outbox-js';

const outbox = new Outbox({
  storage: new LocalStorageAdapter(),
  transport: new FetchTransport('/api/sync'),
});

outbox.enqueue('complete-step', 'job-step', { stepId: 's3', done: true }, 'tech-7');
await outbox.flush(); // retries with backoff, resolves conflicts
```

## Quickstart

```bash
npm ci && npm run build
npm run demo          # http://localhost:8040/demo/
```

In the demo: toggle steps while offline (DevTools → offline, or the in-page switch) → hit **Sync now** → reload mid-queue (it survives) → arm **Conflict on next sync** to see `keep-both` park a mutation for review. (Serve over `http`, not `file://` — ES modules need it.)

## How it works

```
enqueue() → persist → flush() → transport.send() ─┬─ ack      → acked
                                                  ├─ conflict → strategy decides
                                                  └─ throw    → backoff + retry (→ failed after maxAttempts)
```

- FIFO order, crash recovery (`sending` → `pending` on reload), one poisoned mutation never blocks the rest.
- Concurrent-safe: a second `flush()` mid-flight is a no-op; terminal entries are never resent.

## API

| Method | What it does |
|---|---|
| `enqueue(op, entity, payload, actor?)` | Persist + queue a mutation, emits `enqueued` |
| `flush()` | Push pending FIFO with retries, emits `drained` when empty |
| `getQueue` / `getPending` / `getPendingCount` | Inspect (copies, safe to render) |
| `getById(id)` | Status + attempts + error + resolution |
| `resolveConflict(id, 'retry' \| 'drop')` | Manual review for `keep-both` |
| `delayFor(attempt)` | Pure backoff schedule (testable) |
| `prune()` / `clear()` | Drop terminal entries / empty queue |
| `on(event, fn)` | Subscribe (`enqueued, acked, failed, conflict, drained`) |

**Statuses:** `pending → sending → acked | failed | conflict`.

**Storage:** `LocalStorageAdapter` (durable, browsers) · `MemoryStorage` (Node/tests).
**Transports:** `FetchTransport` (production, 409 → conflict) · `FakeTransport` (tests/demo).

**Conflict strategies:** `last-write-wins` (default) · `server-wins` · `keep-both` (parks for human review).

## Repo map

```
src/outbox.ts       Outbox, MemoryStorage, LocalStorageAdapter, backoff, events
src/transports.ts   Transport, FetchTransport, FakeTransport
src/conflicts.ts    last-write-wins / server-wins / keep-both
src/index.ts        public barrel
tests/              outbox.test.ts (29) + transports.test.ts (7)
demo/index.html     single-file checklist (imports ../dist, no bundler)
```

## Tests

```bash
npm test            # 36 tests, Vitest
npm run build       # tsc → dist/ (ESM + .d.ts, zero-dep)
```

## Honest limits

- No IndexedDB adapter yet — `localStorage` ~5MB cap (interface ready).
- No E2E encryption — queued payloads are plaintext.
- LWW needs server timestamps to be exactly right; `keep-both` covers what matters.
- No mutation compaction — ten toggles sync as ten mutations.

## License

MIT — see [LICENSE](./LICENSE).
