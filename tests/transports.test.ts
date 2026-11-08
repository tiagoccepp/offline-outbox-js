import { describe, it, expect } from 'vitest';
import { FakeTransport, FetchTransport, type FetchFn, type Mutation } from '../src/index.js';

const sample = (id = 'm1'): Mutation => ({
  id,
  op: 'update',
  entity: 'job',
  payload: { f: 1 },
  ts: 1000,
  actor: 'tech-7',
});

describe('FakeTransport', () => {
  it('records sent mutations and acks', async () => {
    const t = new FakeTransport();
    const res = await t.send(sample());
    expect(res.status).toBe('acked');
    expect(t.sent.map((m) => m.id)).toEqual(['m1']);
  });

  it('failFirst throws N times then recovers', async () => {
    const t = new FakeTransport({ failFirst: 2 });
    await expect(t.send(sample())).rejects.toThrow(/network down/);
    await expect(t.send(sample())).rejects.toThrow(/network down/);
    await expect(t.send(sample())).resolves.toMatchObject({ status: 'acked' });
  });

  it('failIds only fails the listed ids', async () => {
    const t = new FakeTransport({ failIds: ['bad'] });
    await expect(t.send(sample('bad'))).rejects.toThrow();
    await expect(t.send(sample('good'))).resolves.toMatchObject({ status: 'acked' });
  });

  it('conflictIds reports conflict with server info', async () => {
    const t = new FakeTransport({ conflictIds: ['c1'], conflictServerTs: 999 });
    const res = await t.send(sample('c1'));
    expect(res).toMatchObject({ status: 'conflict', serverTs: 999 });
  });
});

describe('FetchTransport', () => {
  it('POSTs JSON and acks on 200', async () => {
    const calls: Array<{ url: string; body: unknown }> = [];
    const fetchFn: FetchFn = async (url, init) => {
      calls.push({ url, body: init?.body });
      return new Response('{}', { status: 200 });
    };
    const t = new FetchTransport('https://crm.example/api/sync', { fetchFn });
    const res = await t.send(sample());
    expect(res.status).toBe('acked');
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe('https://crm.example/api/sync');
    expect(JSON.parse(calls[0].body as string)).toMatchObject({ id: 'm1', op: 'update' });
  });

  it('maps 409 to conflict with server timestamp', async () => {
    const fetchFn: FetchFn = async () =>
      new Response(JSON.stringify({ serverTs: 4242, version: 7 }), { status: 409 });
    const t = new FetchTransport('https://crm.example/api/sync', { fetchFn });
    const res = await t.send(sample());
    expect(res).toMatchObject({ status: 'conflict', serverTs: 4242 });
  });

  it('throws on 500', async () => {
    const fetchFn: FetchFn = async () => new Response('boom', { status: 500 });
    const t = new FetchTransport('https://crm.example/api/sync', { fetchFn });
    await expect(t.send(sample())).rejects.toThrow(/http 500/);
  });
});
