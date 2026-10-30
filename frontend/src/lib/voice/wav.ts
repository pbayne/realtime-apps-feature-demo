// 16-bit mono PCM WAV encoder. Ported from the reference voice-agent app (index.html `wav()`).
// SPEC §9.2: a binary frame on /ws/voice is one complete user turn as WAV at 16 kHz.

export const VOICE_RATE = 16000;

export function wav(frames: Float32Array[], rate: number = VOICE_RATE): ArrayBuffer {
  const samples = frames.reduce((n, f) => n + f.length, 0);
  const view = new DataView(new ArrayBuffer(44 + samples * 2));
  const text = (offset: number, s: string) => {
    for (let i = 0; i < s.length; i++) view.setUint8(offset + i, s.charCodeAt(i));
  };
  text(0, "RIFF");
  view.setUint32(4, 36 + samples * 2, true);
  text(8, "WAVEfmt ");
  view.setUint32(16, 16, true); // fmt chunk size
  view.setUint16(20, 1, true); // PCM
  view.setUint16(22, 1, true); // mono
  view.setUint32(24, rate, true);
  view.setUint32(28, rate * 2, true); // byte rate
  view.setUint16(32, 2, true); // block align
  view.setUint16(34, 16, true); // bits per sample
  text(36, "data");
  view.setUint32(40, samples * 2, true);
  let o = 44;
  for (const f of frames) {
    for (let i = 0; i < f.length; i++) {
      const x = Math.max(-1, Math.min(1, f[i]));
      view.setInt16(o, Math.round(x * 32767), true);
      o += 2;
    }
  }
  return view.buffer;
}
