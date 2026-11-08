/**
 * Conflict strategies for the offline outbox.
 *
 * A conflict happens when the server already holds a different/newer version
 * of the entity a queued mutation touches (the transport reports `conflict`,
 * typically from an HTTP 409). The strategy decides what happens to the
 * local mutation — no strategy ever throws.
 */

export type ConflictStrategyName = 'last-write-wins' | 'server-wins' | 'keep-both';

export interface ConflictContext {
  localTs: number;
  localId: string;
  /** Server version info, when the transport provides it. */
  serverTs?: number;
  serverVersion?: unknown;
}

export type ConflictResolution =
  | { action: 'ack'; reason: 'local-newer' } // local applied, done
  | { action: 'drop'; reason: 'server-newer' | 'server-wins' } // local discarded, done
  | { action: 'keep'; reason: 'manual-review' }; // parked as `conflict`, human decides

export function resolveConflict(
  strategy: ConflictStrategyName,
  ctx: ConflictContext,
): ConflictResolution {
  switch (strategy) {
    case 'server-wins':
      return { action: 'drop', reason: 'server-wins' };
    case 'keep-both':
      return { action: 'keep', reason: 'manual-review' };
    case 'last-write-wins':
    default: {
      // Honest LWW: compare timestamps when the server provides one.
      // No server timestamp -> local is the only write we know about: it wins.
      if (ctx.serverTs !== undefined && ctx.serverTs > ctx.localTs) {
        return { action: 'drop', reason: 'server-newer' };
      }
      return { action: 'ack', reason: 'local-newer' };
    }
  }
}
