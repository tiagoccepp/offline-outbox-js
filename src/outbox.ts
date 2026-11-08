import { resolveConflict, type ConflictStrategyName } from './conflicts.js';
import type { Transport } from './transports.js';

export type MutationStatus = 'pending' | 'sending' | 'acked' | 'failed' | 'conflict';
export type OutboxEvent = 'enqueued' | 'acked' | 'failed' | 'conflict' | 'drained';

export interface Mutation {
  id: string;
  op: string;
  entity: string;
  payload: unknown;
  ts: number;
  actor: string;
}

export interface StoredMutation extends Mutation {
  status: MutationStatus;
  /** Total tries so far (including the final successful one). */
  attempts: number;
  nextRetryAt: number;
  error?: string;
  /** How it finished: server-ack, local-newer, server-newer, server-wins, manual-review, manual-drop. */
  resolution?: string;
}

export interface StorageAdapter {
  load(): StoredMutation[];
  save(queue: StoredMutation[]): void;
  clear(): void;
}

/** Volatile storage. Default in non-browser runtimes; ideal for tests. */
export class MemoryStorage implements StorageAdapter {
  private data: StoredMutation[] = [];
  load(): StoredMutation[] {
    return this.data.map((m) => ({ ...m }));
  }
  save(queue: StoredMutation[]): void {
    this.data = queue.map((m) => ({ ...m }));
  }
  clear(): void {
    this.data = [];
  }
}

/** Durable storage. Default in browsers; survives reloads and crashes. */
export class LocalStorageAdapter implements StorageAdapter {
  constructor(private readonly key = 'offline-outbox:queue') {}
  load(): StoredMutation[] {
    try {
      const raw = globalThis.localStorage?.getItem(this.key);
      if (!raw) return [];
      const parsed: unknown = JSON.parse(raw);
      return Array.isArray(parsed) ? (parsed as StoredMutation[]) : [];
    } catch {
      return [];
    }
  }
  save(queue: StoredMutation[]): void {
    globalThis.localStorage?.setItem(this.key, JSON.stringify(queue));
  }
  clear(): void {
    globalThis.localStorage?.removeItem(this.key);
  }
}

export interface BackoffConfig {
  baseMs: number;
  maxMs: number;
  maxAttempts: number;
  /** Jitter band 0..1 — 0.2 means each delay is ±20%. */
  jitterRatio: number;
}

export interface OutboxOptions {
  transport: Transport;
  storage?: StorageAdapter;
  conflictStrategy?: ConflictStrategyName;
  backoff?: Partial<BackoffConfig>;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
  idGen?: () => string;
}

export interface FlushSummary {
  processed: number;
  acked: number;
  failed: number;
  conflicts: number;
}

/** Per-mutation events carry the mutation; `drained` carries null. */
export type OutboxListener = (mutation: StoredMutation | null) => void;

const DEFAULT_BACKOFF: BackoffConfig = {
  baseMs: 500,
  maxMs: 30_000,
  maxAttempts: 5,
  jitterRatio: 0.2,
};

function defaultId(): string {
  const c = globalThis.crypto as { randomUUID?: () => string } | undefined;
  if (c?.randomUUID) return c.randomUUID();
  return `${Date.now().toString(36)}-${Math.floor(Math.random() * 1e9).toString(36)}`;
}

/**
 * Framework-free durable outbox. Enqueue mutations offline, flush when online.
 * Zero runtime dependencies; works in browsers, workers, and Node.
 */
export class Outbox {
  private queue: StoredMutation[];
  private readonly storage: StorageAdapter;
  private readonly transport: Transport;
  private readonly strategy: ConflictStrategyName;
  readonly backoff: BackoffConfig;
  private readonly now: () => number;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly random: () => number;
  private readonly idGen: () => string;
  private readonly listeners = new Map<OutboxEvent, Set<OutboxListener>>();
  private flushing = false;

  constructor(opts: OutboxOptions) {
    this.transport = opts.transport;
    this.storage =
      opts.storage ??
      (typeof globalThis.localStorage !== 'undefined'
        ? new LocalStorageAdapter()
        : new MemoryStorage());
    this.strategy = opts.conflictStrategy ?? 'last-write-wins';
    this.backoff = { ...DEFAULT_BACKOFF, ...opts.backoff };
    this.now = opts.now ?? Date.now;
    this.sleep = opts.sleep ?? ((ms) => new Promise<void>((r) => setTimeout(r, ms)));
    this.random = opts.random ?? Math.random;
    this.idGen = opts.idGen ?? defaultId;
    // Crash recovery: anything stuck in `sending` never got an ack.
    this.queue = this.storage
      .load()
      .map((m) => (m.status === 'sending' ? { ...m, status: 'pending' as const } : { ...m }));
    this.persist();
  }

  on(event: OutboxEvent, fn: OutboxListener): () => void {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    set.add(fn);
    return () => {
      set.delete(fn);
    };
  }

  private emit(event: OutboxEvent, payload: StoredMutation | null): void {
    this.listeners.get(event)?.forEach((fn) => fn(payload));
  }

  enqueue(op: string, entity: string, payload: unknown, actor = 'tech'): StoredMutation {
    const m: StoredMutation = {
      id: this.idGen(),
      op,
      entity,
      payload,
      ts: this.now(),
      actor,
      status: 'pending',
      attempts: 0,
      nextRetryAt: 0,
    };
    this.queue.push(m);
    this.persist();
    this.emit('enqueued', { ...m });
    return { ...m };
  }

  /**
   * Delay before retry number `attempt` (0-based):
   * min(maxMs, baseMs * 2^attempt), then ±jitter.
   */
  delayFor(attempt: number): number {
    const { baseMs, maxMs, jitterRatio } = this.backoff;
    const capped = Math.min(maxMs, baseMs * 2 ** attempt);
    if (jitterRatio <= 0) return capped;
    const jitter = (this.random() * 2 - 1) * jitterRatio;
    return Math.max(0, Math.round(capped * (1 + jitter)));
  }

  getQueue(): StoredMutation[] {
    return this.queue.map((m) => ({ ...m }));
  }

  getPending(): StoredMutation[] {
    return this.getQueue().filter((m) => m.status === 'pending' || m.status === 'sending');
  }

  getPendingCount(): number {
    return this.queue.filter((m) => m.status === 'pending' || m.status === 'sending').length;
  }

  getById(id: string): StoredMutation | undefined {
    const m = this.queue.find((x) => x.id === id);
    return m ? { ...m } : undefined;
  }

  /**
   * Push every pending mutation in FIFO order. Each mutation retries
   * internally with backoff; one poisoned mutation never blocks the rest.
   * Safe to call concurrently (second call is a no-op) and to re-call —
   * acked/failed/conflict entries are never resent.
   */
  async flush(): Promise<FlushSummary> {
    if (this.flushing) return { processed: 0, acked: 0, failed: 0, conflicts: 0 };
    this.flushing = true;
    const summary: FlushSummary = { processed: 0, acked: 0, failed: 0, conflicts: 0 };
    try {
      const ids = this.queue.filter((m) => m.status === 'pending').map((m) => m.id);
      for (const id of ids) {
        const current = this.queue.find((m) => m.id === id);
        if (!current || current.status !== 'pending') continue;
        if (current.nextRetryAt > this.now()) continue; // not due yet
        summary.processed += 1;
        await this.sendWithRetry(current, summary);
        this.persist();
      }
      this.persist();
      if (this.getPendingCount() === 0) this.emit('drained', null);
      return summary;
    } finally {
      this.flushing = false;
    }
  }

  private async sendWithRetry(m: StoredMutation, summary: FlushSummary): Promise<void> {
    while (m.attempts < this.backoff.maxAttempts) {
      m.status = 'sending';
      this.persist();
      try {
        const res = await this.transport.send({ ...m });
        if (res.status === 'conflict') {
          const r = resolveConflict(this.strategy, {
            localTs: m.ts,
            localId: m.id,
            serverTs: res.serverTs,
            serverVersion: res.serverVersion,
          });
          if (r.action === 'keep') {
            m.status = 'conflict';
            m.resolution = r.reason;
            summary.conflicts += 1;
            this.emit('conflict', { ...m });
          } else if (r.action === 'drop') {
            m.status = 'acked';
            m.resolution = r.reason;
            summary.acked += 1;
            this.emit('acked', { ...m });
          } else {
            m.status = 'acked';
            m.resolution = r.reason;
            summary.acked += 1;
            this.emit('acked', { ...m });
          }
          return;
        }
        m.status = 'acked';
        m.attempts += 1;
        m.resolution = 'server-ack';
        summary.acked += 1;
        this.emit('acked', { ...m });
        return;
      } catch (err) {
        m.attempts += 1;
        m.error = err instanceof Error ? err.message : String(err);
        if (m.attempts >= this.backoff.maxAttempts) {
          m.status = 'failed';
          summary.failed += 1;
          this.emit('failed', { ...m });
          return;
        }
        m.status = 'pending';
        const delay = this.delayFor(m.attempts - 1);
        m.nextRetryAt = this.now() + delay;
        this.persist();
        await this.sleep(delay);
      }
    }
  }

  /** Manual review for `keep-both` conflicts: retry (requeue) or drop (accept server). */
  resolveConflict(id: string, action: 'retry' | 'drop'): boolean {
    const m = this.queue.find((x) => x.id === id);
    if (!m || m.status !== 'conflict') return false;
    if (action === 'retry') {
      m.status = 'pending';
      m.attempts = 0;
      m.nextRetryAt = 0;
      m.error = undefined;
      m.resolution = undefined;
    } else {
      m.status = 'acked';
      m.resolution = 'manual-drop';
      this.emit('acked', { ...m });
    }
    this.persist();
    return true;
  }

  /** Drop terminal entries (acked/failed). Returns the removed count. */
  prune(): number {
    const before = this.queue.length;
    this.queue = this.queue.filter((m) => m.status !== 'acked' && m.status !== 'failed');
    this.persist();
    return before - this.queue.length;
  }

  clear(): void {
    this.queue = [];
    this.persist();
  }

  private persist(): void {
    this.storage.save(this.queue);
  }
}
