import type { Mutation } from './outbox.js';

/**
 * Result of pushing one mutation to the server.
 * A thrown error means transient failure (the outbox retries with backoff).
 */
export type TransportResult =
  | { status: 'acked'; serverTs?: number }
  | { status: 'conflict'; serverTs?: number; serverVersion?: unknown };

/** Pushes mutations to a backend. Implement this interface for your API. */
export interface Transport {
  send(mutation: Mutation): Promise<TransportResult>;
}

/** Minimal fetch shape — the real `fetch` is assignable, and so are stubs. */
export type FetchFn = (
  url: string,
  init?: { method?: string; headers?: Record<string, string>; body?: string },
) => Promise<{ ok: boolean; status: number; json(): Promise<unknown> }>;

export interface FetchTransportOptions {
  fetchFn?: FetchFn;
  /** HTTP statuses that mean "conflict" rather than failure. Default: [409]. */
  conflictStatuses?: number[];
}

/** Real transport: POSTs each mutation as JSON to your sync endpoint. */
export class FetchTransport implements Transport {
  private readonly fetchFn: FetchFn;
  private readonly conflictStatuses: number[];

  constructor(
    private readonly url: string,
    opts: FetchTransportOptions = {},
  ) {
    this.fetchFn =
      opts.fetchFn ??
      ((url: string, init?: { method?: string; headers?: Record<string, string>; body?: string }) => {
        const f = globalThis.fetch;
        if (!f) throw new Error('fetch-transport: no global fetch — pass { fetchFn } explicitly');
        return f(url, init) as unknown as Promise<{
          ok: boolean;
          status: number;
          json(): Promise<unknown>;
        }>;
      });
    this.conflictStatuses = opts.conflictStatuses ?? [409];
  }

  async send(mutation: Mutation): Promise<TransportResult> {
    const res = await this.fetchFn(this.url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(mutation),
    });
    if (this.conflictStatuses.includes(res.status)) {
      let serverTs: number | undefined;
      let serverVersion: unknown;
      try {
        const body = (await res.json()) as { serverTs?: unknown; version?: unknown };
        if (typeof body?.serverTs === 'number') serverTs = body.serverTs;
        serverVersion = body?.version ?? body;
      } catch {
        // Conflict body is optional; the strategy still applies.
      }
      return { status: 'conflict', serverTs, serverVersion };
    }
    if (!res.ok) throw new Error(`fetch-transport: http ${res.status} for ${mutation.id}`);
    return { status: 'acked' };
  }
}

export interface FakeTransportOptions {
  latencyMs?: number;
  /** Throw for the first N send() calls, then behave. */
  failFirst?: number;
  alwaysFail?: boolean;
  /** Always throw for these mutation ids (partial-failure drills). */
  failIds?: string[];
  /** Report a conflict for these mutation ids. */
  conflictIds?: string[];
  conflictServerTs?: number;
  onSend?: (mutation: Mutation) => void;
}

/** In-memory transport for tests and the demo. No network, ever. */
export class FakeTransport implements Transport {
  readonly sent: Mutation[] = [];
  attempts = 0;
  private readonly latencyMs: number;
  private readonly failFirst: number;
  private readonly alwaysFail: boolean;
  private readonly failIds: Set<string>;
  private readonly conflictIds: Set<string>;
  private readonly conflictServerTs?: number;
  private readonly onSend?: (mutation: Mutation) => void;

  constructor(opts: FakeTransportOptions = {}) {
    this.latencyMs = opts.latencyMs ?? 0;
    this.failFirst = opts.failFirst ?? 0;
    this.alwaysFail = opts.alwaysFail ?? false;
    this.failIds = new Set(opts.failIds ?? []);
    this.conflictIds = new Set(opts.conflictIds ?? []);
    this.conflictServerTs = opts.conflictServerTs;
    this.onSend = opts.onSend;
  }

  async send(mutation: Mutation): Promise<TransportResult> {
    this.attempts += 1;
    if (this.latencyMs > 0) await new Promise<void>((r) => setTimeout(r, this.latencyMs));
    this.onSend?.(mutation);
    if (this.alwaysFail || this.attempts <= this.failFirst || this.failIds.has(mutation.id)) {
      throw new Error(`fake-transport: network down (attempt ${this.attempts}, id ${mutation.id})`);
    }
    if (this.conflictIds.has(mutation.id)) {
      return {
        status: 'conflict',
        serverTs: this.conflictServerTs,
        serverVersion: { note: 'fake server version' },
      };
    }
    this.sent.push({ ...mutation });
    return { status: 'acked', serverTs: Date.now() };
  }

  reset(): void {
    this.sent.length = 0;
    this.attempts = 0;
  }
}
