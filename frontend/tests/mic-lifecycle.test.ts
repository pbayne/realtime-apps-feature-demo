// Mic lifecycle: a teardown during the permission prompt, or a failed worklet load, must never leave the
// microphone (or an AudioContext) live. Run: `cd frontend && bun test tests/`.
import { describe, expect, mock, test } from "bun:test";

type Deferred<T> = { promise: Promise<T>; resolve(v: T): void };
function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

function fakeStream() {
  const track = { stop: mock(() => {}), enabled: true };
  return { track, stream: { getTracks: () => [track], getAudioTracks: () => [track] } };
}

class FakeCtx {
  static last: FakeCtx | null = null;
  static failWorklet = false;
  sampleRate = 16000;
  close = mock(() => Promise.resolve());
  audioWorklet = { addModule: () => (FakeCtx.failWorklet ? Promise.reject(new Error("worklet 404")) : Promise.resolve()) };
  constructor() {
    FakeCtx.last = this;
  }
  createMediaStreamSource() {
    return { connect() {}, disconnect() {} };
  }
  createAnalyser() {
    return { fftSize: 0, connect() {}, getByteTimeDomainData() {} };
  }
  resume() {
    return Promise.resolve();
  }
}

function installBrowser(getUserMedia: () => Promise<unknown>) {
  const g = globalThis as Record<string, unknown>;
  g.navigator = { mediaDevices: { getUserMedia } };
  g.AudioContext = FakeCtx;
  g.AudioWorkletNode = class {
    port = { onmessage: null };
    connect() {}
    disconnect() {}
  };
  g.URL = Object.assign(URL, { createObjectURL: () => "blob:x", revokeObjectURL: () => {} });
}

describe("startCapture / startMicCapture release the mic when the worklet fails", () => {
  test("voice capture", async () => {
    const { track, stream } = fakeStream();
    installBrowser(() => Promise.resolve(stream));
    FakeCtx.failWorklet = true;
    const { startCapture } = await import("../src/lib/voice/capture");
    await expect(startCapture(() => {})).rejects.toThrow("worklet 404");
    expect(track.stop).toHaveBeenCalledTimes(1);
    expect(FakeCtx.last!.close).toHaveBeenCalledTimes(1);
    FakeCtx.failWorklet = false;
  });

  test("call-console capture", async () => {
    const { track, stream } = fakeStream();
    installBrowser(() => Promise.resolve(stream));
    FakeCtx.failWorklet = true;
    const { startMicCapture } = await import("../src/lib/audio");
    await expect(startMicCapture(() => {})).rejects.toThrow("worklet 404");
    expect(track.stop).toHaveBeenCalledTimes(1);
    expect(FakeCtx.last!.close).toHaveBeenCalledTimes(1);
    FakeCtx.failWorklet = false;
  });
});

describe("MicEngine", () => {
  test("stop() during a pending start releases the late capture", async () => {
    const { track, stream } = fakeStream();
    const prompt = deferred<unknown>();
    installBrowser(() => prompt.promise);
    const { MicEngine } = await import("../src/lib/voice/mic");
    const engine = new MicEngine(() => ({}) as never, () => {});
    const started = engine.start();
    engine.stop();                         // user hits End while the permission prompt is up
    prompt.resolve(stream);                // ...then grants it
    await started;
    expect(track.stop).toHaveBeenCalledTimes(1);
    expect(FakeCtx.last!.close).toHaveBeenCalledTimes(1);
  });
});
