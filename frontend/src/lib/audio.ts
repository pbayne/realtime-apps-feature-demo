// Mic capture -> AudioWorklet (downsample to 16 kHz mono PCM16, 100 ms frames).

export interface MicCapture {
  stop(): void;
}

export async function startMicCapture(onFrame: (pcm: ArrayBuffer) => void): Promise<MicCapture> {
  const stream = await navigator.mediaDevices.getUserMedia({
    audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true },
  });
  let ctx: AudioContext | undefined;
  let source: MediaStreamAudioSourceNode;
  let node: AudioWorkletNode;
  try {
    ctx = new AudioContext();
    await ctx.audioWorklet.addModule("/pcm-worklet.js");
    source = ctx.createMediaStreamSource(stream);
    node = new AudioWorkletNode(ctx, "pcm-downsampler", {
      numberOfInputs: 1,
      numberOfOutputs: 0,
      channelCount: 1,
    });
  } catch (err) {
    // A failed worklet load must not leave the mic (and an AudioContext) live.
    stream.getTracks().forEach((t) => t.stop());
    void ctx?.close().catch(() => {});
    throw err;
  }
  const audioCtx = ctx;
  node.port.onmessage = (e: MessageEvent<ArrayBuffer>) => onFrame(e.data);
  source.connect(node);

  return {
    stop() {
      node.port.onmessage = null;
      source.disconnect();
      node.disconnect();
      stream.getTracks().forEach((t) => t.stop());
      void audioCtx.close();
    },
  };
}
