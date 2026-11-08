import { describe, it, expect, vi, afterEach } from 'vitest';
import {
  Outbox,
  MemoryStorage,
  LocalStorageAdapter,
  FakeTransport,
  type StoredMutation,
} from '../src/index.js';

function localStorageMock(seed: Record<string, string> = {}) {
  const store: Record<string, string> = { ...seed };
  return {
    getItem: (k: string) => (k in store ? store[k] : null),
    setItem: (k: string, v: string) => {
      store[k] = String(v);
    },
    removeItem: (k: string) => {
      delete store[k];
    },
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

const stored = (over: Partial<StoredMutation> = {}): StoredMutation => ({
  id: over.id ?? 'm1',
  op: 'toggle-step',
  entity: 'job-step',
  payload: { stepId: 's1', done: true },
  ts: 1000,
  actor: 'tech-7',
  status: 'pending',
  attempts: 0,
  nextRetryAt: 0,
  ...over,
});

describe('enqueue & order', () => {
  it('assigns id, ts, pending status and zero attempts', () => {
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: new FakeTransport(),
      now: () => 1234,
    });
    const m = o.enqueue('toggle-step', 'job-step', { stepId: 's1' }, 'tech-7');
    expect(m.id).toBeTruthy();
    expect(m.ts).toBe(1234);
    expect(m.status).toBe('pending');
    expect(m.attempts).toBe(0);
    expect(m.actor).toBe('tech-7');
  });

  it('preserves FIFO order across enqueues', () => {
    const o = new Outbox({ storage: new MemoryStorage(), transport: new FakeTransport() });
    const a = o.enqueue('op', 'e', { n: 1 });
    const b = o.enqueue('op', 'e', { n: 2 });
    const c = o.enqueue('op', 'e', { n: 3 });
    expect(o.getQueue().map((m) => m.id)).toEqual([a.id, b.id, c.id]);
  });

  it('emits `enqueued` with the mutation', () => {
    const o = new Outbox({ storage: new MemoryStorage(), transport: new FakeTransport() });
    const seen: StoredMutation[] = [];
    o.on('enqueued', (m) => {
      if (m) seen.push(m);
    });
    const m = o.enqueue('op', 'e', {});
    expect(seen.map((x) => x.id)).toEqual([m.id]);
  });

  it('tracks pending count', () => {
    const o = new Outbox({ storage: new MemoryStorage(), transport: new FakeTransport() });
    expect(o.getPendingCount()).toBe(0);
    o.enqueue('op', 'e', {});
    o.enqueue('op', 'e', {});
    expect(o.getPendingCount()).toBe(2);
  });
});

describe('persistence', () => {
  it('memory storage round-trips the queue across instances', () => {
    const storage = new MemoryStorage();
    const a = new Outbox({ storage, transport: new FakeTransport() });
    a.enqueue('op', 'e', { n: 1 });
    a.enqueue('op', 'e', { n: 2 });
    const b = new Outbox({ storage, transport: new FakeTransport() });
    expect(b.getQueue()).toHaveLength(2);
    expect(b.getPendingCount()).toBe(2);
  });

  it('localStorage adapter persists + reloads the queue', () => {
    vi.stubGlobal('localStorage', localStorageMock());
    const a = new Outbox({
      storage: new LocalStorageAdapter('test:queue'),
      transport: new FakeTransport(),
    });
    a.enqueue('toggle-step', 'job-step', { stepId: 's9' }, 'tech-7');
    const b = new Outbox({
      storage: new LocalStorageAdapter('test:queue'),
      transport: new FakeTransport(),
    });
    expect(b.getQueue()).toHaveLength(1);
    expect(b.getQueue()[0].payload).toEqual({ stepId: 's9' });
  });

  it('localStorage adapter returns [] on corrupt JSON', () => {
    vi.stubGlobal('localStorage', localStorageMock({ 'test:queue': '{nope' }));
    const adapter = new LocalStorageAdapter('test:queue');
    expect(adapter.load()).toEqual([]);
    const o = new Outbox({ storage: adapter, transport: new FakeTransport() });
    expect(o.getQueue()).toEqual([]);
  });

  it('crash recovery flips `sending` back to `pending` on load', () => {
    const storage = new MemoryStorage();
    storage.save([stored({ id: 'stuck', status: 'sending', attempts: 1 })]);
    const o = new Outbox({ storage, transport: new FakeTransport() });
    expect(o.getById('stuck')?.status).toBe('pending');
  });
});

describe('flush & ack', () => {
  it('flush acks every mutation against a healthy transport', async () => {
    const t = new FakeTransport();
    const o = new Outbox({ storage: new MemoryStorage(), transport: t });
    o.enqueue('op', 'e', { n: 1 });
    o.enqueue('op', 'e', { n: 2 });
    const s = await o.flush();
    expect(s).toMatchObject({ processed: 2, acked: 2, failed: 0, conflicts: 0 });
    expect(o.getPendingCount()).toBe(0);
    expect(t.sent).toHaveLength(2);
  });

  it('flush emits `acked` per mutation and `drained` at the end', async () => {
    const o = new Outbox({ storage: new MemoryStorage(), transport: new FakeTransport() });
    const acked: string[] = [];
    let drained = 0;
    o.on('acked', (m) => {
      if (m) acked.push(m.id);
    });
    o.on('drained', () => {
      drained += 1;
    });
    const a = o.enqueue('op', 'e', {});
    const b = o.enqueue('op', 'e', {});
    await o.flush();
    expect(acked).toEqual([a.id, b.id]);
    expect(drained).toBe(1);
  });

  it('flush on an empty queue still emits `drained`', async () => {
    const o = new Outbox({ storage: new MemoryStorage(), transport: new FakeTransport() });
    let drained = 0;
    o.on('drained', () => {
      drained += 1;
    });
    const s = await o.flush();
    expect(s.processed).toBe(0);
    expect(drained).toBe(1);
  });

  it('second flush does not resend acked mutations', async () => {
    const t = new FakeTransport();
    const o = new Outbox({ storage: new MemoryStorage(), transport: t });
    o.enqueue('op', 'e', {});
    await o.flush();
    expect(t.attempts).toBe(1);
    const s = await o.flush();
    expect(s.processed).toBe(0);
    expect(t.attempts).toBe(1);
  });
});

describe('backoff', () => {
  const backoffBox = () =>
    new Outbox({
      storage: new MemoryStorage(),
      transport: new FakeTransport(),
      backoff: { baseMs: 500, maxMs: 4000, jitterRatio: 0 },
    });

  it('delayFor grows exponentially when jitter is off', () => {
    const o = backoffBox();
    expect(o.delayFor(0)).toBe(500);
    expect(o.delayFor(1)).toBe(1000);
    expect(o.delayFor(2)).toBe(2000);
    expect(o.delayFor(3)).toBe(4000);
  });

  it('delayFor caps at maxMs', () => {
    const o = backoffBox();
    expect(o.delayFor(10)).toBe(4000);
  });

  it('jitter stays within ±ratio', () => {
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: new FakeTransport(),
      backoff: { baseMs: 1000, maxMs: 60_000, jitterRatio: 0.2 },
    });
    for (let i = 0; i < 100; i += 1) {
      const d = o.delayFor(1); // nominal 2000
      expect(d).toBeGreaterThanOrEqual(1600);
      expect(d).toBeLessThanOrEqual(2400);
    }
  });

  it('retry-then-ack: fails twice, then acks (fake timers)', async () => {
    vi.useFakeTimers();
    try {
      const t = new FakeTransport({ failFirst: 2 });
      const o = new Outbox({
        storage: new MemoryStorage(),
        transport: t,
        backoff: { baseMs: 1000, maxMs: 10_000, maxAttempts: 5, jitterRatio: 0 },
      });
      const m = o.enqueue('op', 'e', {});
      const p = o.flush();
      await vi.advanceTimersByTimeAsync(10_000);
      const s = await p;
      expect(s.acked).toBe(1);
      expect(t.attempts).toBe(3);
      expect(o.getById(m.id)?.status).toBe('acked');
      expect(o.getById(m.id)?.attempts).toBe(3);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('retry exhausted', () => {
  it('marks failed after maxAttempts and keeps the error', async () => {
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: new FakeTransport({ alwaysFail: true }),
      backoff: { baseMs: 1, maxAttempts: 3, jitterRatio: 0 },
    });
    const m = o.enqueue('op', 'e', {});
    const s = await o.flush();
    expect(s.failed).toBe(1);
    const got = o.getById(m.id);
    expect(got?.status).toBe('failed');
    expect(got?.attempts).toBe(3);
    expect(got?.error).toMatch(/network down/);
  });

  it('emits `failed` (and `drained`: nothing pending is left)', async () => {
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: new FakeTransport({ alwaysFail: true }),
      backoff: { baseMs: 1, maxAttempts: 2, jitterRatio: 0 },
    });
    const failed: string[] = [];
    let drained = 0;
    o.on('failed', (m) => {
      if (m) failed.push(m.id);
    });
    o.on('drained', () => {
      drained += 1;
    });
    const m = o.enqueue('op', 'e', {});
    await o.flush();
    expect(failed).toEqual([m.id]);
    expect(drained).toBe(1);
  });
});

describe('conflicts', () => {
  it('default strategy is last-write-wins (local wins without server ts)', async () => {
    const t = new FakeTransport({ conflictIds: ['c1'] });
    const o = new Outbox({ storage: new MemoryStorage(), transport: t, idGen: () => 'c1' });
    const m = o.enqueue('op', 'e', {});
    const s = await o.flush();
    expect(s.acked).toBe(1);
    expect(o.getById(m.id)?.resolution).toBe('local-newer');
  });

  it('lww: local newer than server -> acked as local-newer', async () => {
    const t = new FakeTransport({ conflictIds: ['c1'], conflictServerTs: 500 });
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: t,
      idGen: () => 'c1',
      now: () => 1000,
    });
    o.enqueue('op', 'e', {});
    await o.flush();
    expect(o.getById('c1')?.status).toBe('acked');
    expect(o.getById('c1')?.resolution).toBe('local-newer');
  });

  it('lww: server newer than local -> dropped, resolution server-newer', async () => {
    const t = new FakeTransport({ conflictIds: ['c1'], conflictServerTs: 5000 });
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: t,
      idGen: () => 'c1',
      now: () => 1000,
    });
    const evts: string[] = [];
    o.on('acked', () => evts.push('acked'));
    o.on('conflict', () => evts.push('conflict'));
    o.enqueue('op', 'e', {});
    const s = await o.flush();
    expect(s.acked).toBe(1);
    expect(o.getById('c1')?.resolution).toBe('server-newer');
    expect(evts).toEqual(['acked']);
  });

  it('server-wins drops local and emits acked', async () => {
    const t = new FakeTransport({ conflictIds: ['c1'] });
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: t,
      conflictStrategy: 'server-wins',
      idGen: () => 'c1',
    });
    const acked: string[] = [];
    o.on('acked', (m) => {
      if (m) acked.push(m.id);
    });
    o.enqueue('op', 'e', {});
    await o.flush();
    expect(o.getById('c1')?.status).toBe('acked');
    expect(o.getById('c1')?.resolution).toBe('server-wins');
    expect(acked).toEqual(['c1']);
  });

  it('keep-both parks the mutation as `conflict` and emits conflict', async () => {
    const t = new FakeTransport({ conflictIds: ['c1'] });
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: t,
      conflictStrategy: 'keep-both',
      idGen: () => 'c1',
    });
    const conflicts: string[] = [];
    o.on('conflict', (m) => {
      if (m) conflicts.push(m.id);
    });
    o.enqueue('op', 'e', {});
    const s = await o.flush();
    expect(s.conflicts).toBe(1);
    expect(o.getById('c1')?.status).toBe('conflict');
    expect(conflicts).toEqual(['c1']);
    expect(t.sent).toHaveLength(0);
  });

  it('manual resolve: retry requeues, drop accepts server', async () => {
    let n = 0;
    const t = new FakeTransport({ conflictIds: ['c1', 'c2'] });
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: t,
      conflictStrategy: 'keep-both',
      idGen: () => `c${(n += 1)}`,
    });
    o.enqueue('op', 'e', {});
    o.enqueue('op', 'e', {});
    await o.flush();
    expect(o.resolveConflict('c1', 'retry')).toBe(true);
    expect(o.getById('c1')?.status).toBe('pending');
    expect(o.getById('c1')?.attempts).toBe(0);
    expect(o.resolveConflict('c2', 'drop')).toBe(true);
    expect(o.getById('c2')?.status).toBe('acked');
    expect(o.getById('c2')?.resolution).toBe('manual-drop');
    expect(o.resolveConflict('nope', 'retry')).toBe(false);
  });
});

describe('partial flush & events', () => {
  it('one poisoned mutation does not block the rest', async () => {
    let n = 0;
    const ids = ['poison', 'healthy'];
    const t = new FakeTransport({ failIds: ['poison'] });
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: t,
      idGen: () => ids[n++],
      backoff: { baseMs: 1, maxAttempts: 2, jitterRatio: 0 },
    });
    o.enqueue('op', 'e', {});
    o.enqueue('op', 'e', {});
    const s = await o.flush();
    expect(s).toMatchObject({ processed: 2, acked: 1, failed: 1 });
    expect(o.getById('poison')?.status).toBe('failed');
    expect(o.getById('healthy')?.status).toBe('acked');
  });

  it('flush skips parked conflicts until manually resolved', async () => {
    const t = new FakeTransport({ conflictIds: ['c1'] });
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: t,
      conflictStrategy: 'keep-both',
      idGen: () => 'c1',
    });
    o.enqueue('op', 'e', {});
    await o.flush();
    const attemptsAfterFirst = t.attempts;
    const s = await o.flush();
    expect(s.processed).toBe(0);
    expect(t.attempts).toBe(attemptsAfterFirst);
    expect(o.getById('c1')?.status).toBe('conflict');
  });

  it('unsubscribe stops events', () => {
    const o = new Outbox({ storage: new MemoryStorage(), transport: new FakeTransport() });
    let n = 0;
    const off = o.on('enqueued', () => {
      n += 1;
    });
    o.enqueue('op', 'e', {});
    off();
    o.enqueue('op', 'e', {});
    expect(n).toBe(1);
  });

  it('concurrent flush calls do not double-send', async () => {
    const t = new FakeTransport({ latencyMs: 5 });
    const o = new Outbox({ storage: new MemoryStorage(), transport: t });
    o.enqueue('op', 'e', {});
    const [s1, s2] = await Promise.all([o.flush(), o.flush()]);
    expect(s1.processed + s2.processed).toBe(1);
    expect(t.attempts).toBe(1);
  });

  it('prune drops terminal entries and keeps the rest', async () => {
    const t = new FakeTransport({ alwaysFail: true });
    const o = new Outbox({
      storage: new MemoryStorage(),
      transport: t,
      backoff: { baseMs: 1, maxAttempts: 1, jitterRatio: 0 },
    });
    o.enqueue('op', 'e', {});
    await o.flush();
    o.enqueue('op', 'e', {});
    expect(o.prune()).toBe(1);
    expect(o.getQueue()).toHaveLength(1);
    expect(o.getPendingCount()).toBe(1);
  });
});
