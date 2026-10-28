export function Brand() {
  return (
    <div className="flex items-center gap-2.5">
      <div className="grid size-7 place-items-center rounded-lg bg-[var(--color-lava)] shadow-[0_4px_18px_-6px_rgba(255,54,33,0.9)]">
        <span className="block size-2.5 rounded-sm bg-white/90" />
      </div>
      <div className="leading-tight">
        <div className="text-[15px] font-semibold tracking-tight">Call Copilot</div>
        <div className="text-[11px] text-[var(--color-muted)]">Realtime assist</div>
      </div>
    </div>
  );
}
