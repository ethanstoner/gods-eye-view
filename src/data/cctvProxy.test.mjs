import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  CCTV_FRAME_FETCH_TIMEOUT_MS,
  CCTV_FRAME_MAX_BODY_BYTES,
  fetchCctvImageFromUpstream,
} from '../../vite.config.js';

/** A body that arrives in chunks and never declares a Content-Length. */
function chunkedImageResponse(chunkBytes, chunkCount, { onChunk = () => {}, onCancel = () => {} } = {}) {
  const stream = new ReadableStream({
    async pull(controller) {
      if (chunkCount <= 0) {
        controller.close();
        return;
      }
      chunkCount -= 1;
      onChunk();
      controller.enqueue(new Uint8Array(chunkBytes));
    },
    cancel: onCancel,
  });
  return new Response(stream, { status: 200, headers: { 'Content-Type': 'image/jpeg' } });
}

test('CCTV upstream frame fetch supplies a bounded abort signal', async () => {
  let observedSignal = null;
  const startedAt = Date.now();
  const result = await fetchCctvImageFromUpstream('https://example.com/frame.jpg', {
    timeoutMs: 20,
    fetchImpl: (_url, options) => new Promise((_resolve, reject) => {
      observedSignal = options.signal;
      options.signal.addEventListener('abort', () => reject(options.signal.reason), { once: true });
    }),
  });

  assert.equal(result, null);
  assert.ok(observedSignal instanceof AbortSignal);
  assert.equal(observedSignal.aborted, true);
  assert.ok(Date.now() - startedAt < 500, 'test timeout should settle promptly');
  assert.ok(CCTV_FRAME_FETCH_TIMEOUT_MS < 10_000, 'production timeout must beat the active refresh cadence');
});

test('CCTV upstream frame fetch returns a valid image response', async () => {
  const result = await fetchCctvImageFromUpstream('https://example.com/frame.jpg', {
    timeoutMs: 100,
    fetchImpl: async () => new Response(Uint8Array.from([1, 2, 3]), {
      status: 200,
      headers: { 'Content-Type': 'image/jpeg' },
    }),
  });

  assert.equal(result?.ok, true);
  assert.equal(result?.contentType, 'image/jpeg');
  assert.deepEqual(result?.body, Buffer.from([1, 2, 3]));
});

test('CCTV upstream frame fetch rejects a declared oversize body without draining it', async () => {
  const chunkBytes = 64 * 1024;
  let pulled = 0;
  let cancelled = false;
  const result = await fetchCctvImageFromUpstream('https://example.com/frame.jpg', {
    timeoutMs: 1000,
    maxBytes: chunkBytes * 4,
    fetchImpl: async () => {
      const response = chunkedImageResponse(chunkBytes, 64, {
        onChunk: () => { pulled += 1; },
        onCancel: () => { cancelled = true; },
      });
      response.headers.set('Content-Length', String(chunkBytes * 64));
      return response;
    },
  });

  // Assert on the byte count, not the object: a regressed proxy returns a
  // multi-megabyte Buffer here, and diffing one into the failure report is
  // slower than the check it is reporting on.
  assert.equal(result?.body?.length ?? null, null, 'a declared oversize snapshot is a miss, not a buffered body');
  assert.equal(cancelled, true, 'the declared cap is enforced by cancelling, not reading');
  // Only the stream's own one-chunk prefetch may have run; the proxy pulls none.
  assert.ok(pulled <= 1, `declared cap short-circuits the read, pulled ${pulled} chunks`);
});

test('CCTV upstream frame fetch aborts an undeclared body once it crosses the cap', async () => {
  const chunkBytes = 64 * 1024;
  let pulled = 0;
  let cancelled = false;
  const result = await fetchCctvImageFromUpstream('https://example.com/frame.jpg', {
    timeoutMs: 1000,
    maxBytes: chunkBytes * 4,
    // Sixty-four chunks are offered with no Content-Length; a proxy that
    // buffers first would take all of them.
    fetchImpl: async () => chunkedImageResponse(chunkBytes, 64, {
      onChunk: () => { pulled += 1; },
      onCancel: () => { cancelled = true; },
    }),
  });

  assert.equal(result?.body?.length ?? null, null, 'a chunked body over the cap is a miss');
  // Four chunks fit, the fifth crosses the cap, and one more sits in the
  // stream's prefetch queue — nothing past that is ever pulled.
  assert.ok(pulled <= 6, `stopped reading at the cap, pulled ${pulled} chunks`);
  assert.equal(cancelled, true, 'the upstream stream is cancelled, not drained');
});

test('CCTV upstream frame fetch still returns a chunked body under the cap', async () => {
  const chunkBytes = 1024;
  const result = await fetchCctvImageFromUpstream('https://example.com/frame.jpg', {
    timeoutMs: 1000,
    maxBytes: chunkBytes * 8,
    fetchImpl: async () => chunkedImageResponse(chunkBytes, 3),
  });

  assert.equal(result?.ok, true);
  assert.equal(result?.body?.length, chunkBytes * 3, 'every chunk is reassembled in order');
  assert.ok(CCTV_FRAME_MAX_BODY_BYTES > 0 && CCTV_FRAME_MAX_BODY_BYTES <= 64 * 1024 * 1024,
    'the frame cap must be at or under the media route cap');
});
