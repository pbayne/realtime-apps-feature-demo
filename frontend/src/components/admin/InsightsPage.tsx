import { useEffect, useMemo, useState, type ReactNode } from "react";

// Shape of GET /api/admin/insights (src/app/admin/insights.py). Every section can be empty; `configured` false is the
// empty state for an app without a SQL warehouse.
type Row = Record<string, string | number | null>;
interface Overview {
  configured: boolean;
  reason?: string;
  days: number;
  as_of_ms: number;
  sections: Record<string, Row[]>;
  errors?: Record<string, string>;
}
interface CallDetail {
  call_id: string;
  configured: boolean;
  utterances: Row[];
  cards: Row[];
  summary: Row | null;
}

const WINDOWS = [7, 30, 90] as const;

async function getJson<T>(path: string): Promise<T> {
  const r = await fetch(path);
  if (!r.ok) throw new Error(r.status === 403 ? "Insights are for admins." : `HTTP ${r.status}`);
  return (await r.json()) as T;
}

const num = (v: unknown): number => (typeof v === "number" ? v : Number(v ?? 0)) || 0;
const fmt = (v: number, d = 0) => (Number.isFinite(v) ? v.toFixed(d) : "–");

export function InsightsPage() {
  const [days, setDays] = useState<number>(7);
  const [data, setData] = useState<Overview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [openCall, setOpenCall] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setData(null);
    setError(null);
    getJson<Overview>(`/api/admin/insights?days=${days}`)
      .then((d) => alive && setData(d))
      .catch((e: Error) => alive && setError(e.message));
    return () => {
      alive = false;
    };
  }, [days]);

  const s = data?.sections ?? {};
  const k = useMemo(() => kpis(s), [s]);

  return (
    <section aria-labelledby="ins-h">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="kbd-label">Admin</p>
          <h1 id="ins-h" className="mt-2 text-[28px] font-semibold leading-tight tracking-tight">
            Insights
          </h1>
          <p className="mt-2 max-w-2xl text-[14px] leading-relaxed text-[var(--color-muted)]">
            Calls, transcripts and cards from the Delta history tables. Cached for a minute.
          </p>
        </div>
        <div role="group" aria-label="Time window" className="flex gap-1 rounded-[var(--radius-control)] border border-[var(--color-line)] p-1">
          {WINDOWS.map((w) => (
            <button
              key={w}
              type="button"
              aria-pressed={days === w}
              onClick={() => setDays(w)}
              className={`rounded-[calc(var(--radius-control)-3px)] px-3 py-1 text-[12.5px] font-semibold ${
                days === w ? "bg-[var(--color-raised)] text-[var(--color-text)]" : "text-[var(--color-muted)]"
              }`}
            >
              {w} days
            </button>
          ))}
        </div>
      </div>

      {error && (
        <p role="alert" className="mt-6 rounded-[var(--radius-control)] border border-[var(--c-neg)]/40 p-3 text-[13px] text-[var(--c-neg)]">
          {error}
        </p>
      )}
      {!error && !data && <p className="mt-6 text-[13px] text-[var(--color-muted)]">Loading…</p>}

      {data && !data.configured && (
        <div className="panel mt-6 p-6">
          <p className="text-[15px] font-semibold">No SQL warehouse connected</p>
          <p className="mt-2 text-[13.5px] leading-relaxed text-[var(--color-muted)]">
            {data.reason} Add the sql-warehouse app resource and redeploy. The page fills in once the history tables have rows.
          </p>
        </div>
      )}

      {data?.configured && (
        <>
          {/* KPI tiles */}
          <div className="mt-6 grid gap-3 sm:grid-cols-3 lg:grid-cols-6">
            <Tile label="Calls" value={fmt(k.calls)} />
            <Tile label="Voice sessions" value={fmt(k.voice)} />
            <Tile label="Avg call length" value={`${fmt(k.avgMin, 1)} min`} />
            <Tile label="Latency p50" value={`${fmt(k.p50)} ms`} hint="final to enrichment" />
            <Tile label="Latency p90" value={`${fmt(k.p90)} ms`} />
            <Tile label="Avg sentiment" value={fmt(k.sentiment, 2)} hint="-1 negative, +1 positive" />
          </div>

          {/* Charts */}
          <div className="mt-4 grid gap-4 lg:grid-cols-2">
            <Card title="Calls per day">
              <BarChart rows={s.calls_daily ?? []} xKey="day" yKey="calls" />
            </Card>
            <Card title="Customer sentiment per day">
              <LineChart rows={s.sentiment_daily ?? []} xKey="day" yKey="avg_sentiment" min={-1} max={1} />
            </Card>
          </div>

          {/* Lists */}
          <div className="mt-4 grid gap-4 md:grid-cols-3">
            <Card title="Top objections">
              <BarList rows={s.top_objections ?? []} />
            </Card>
            <Card title="Top intents">
              <BarList rows={s.top_intents ?? []} />
            </Card>
            <Card title="Risk flags">
              <BarList rows={s.top_risk_flags ?? []} />
            </Card>
          </div>

          <div className="mt-4 grid gap-4 md:grid-cols-2">
            <Card title="Card outcomes by kind">
              <Outcomes rows={s.card_outcomes ?? []} />
            </Card>
            <Card title="Talk-track gaps (latest run)">
              {(s.talk_tracks ?? []).length === 0 ? (
                <p className="text-[13px] text-[var(--color-muted)]">
                  Not generated yet. The talk-track insights job writes this table each day.
                </p>
              ) : (
                <ul className="space-y-2 text-[13px]">
                  {(s.talk_tracks ?? []).slice(0, 8).map((t, i) => (
                    <li key={i} className="rounded-[var(--radius-control)] border border-[var(--color-line)] p-2.5">
                      <span className="chip chip-neutral mr-2">{String(t.kind)}</span>
                      <span className="font-semibold">{String(t.label)}</span>
                      <p className="mt-1 text-[var(--color-muted)]">{String(t.detail ?? "")}</p>
                    </li>
                  ))}
                </ul>
              )}
            </Card>
          </div>

          {/* Recent calls with drill-down */}
          <div className="mt-4">
            <Card title="Recent calls">
              {(s.recent_calls ?? []).length === 0 ? (
                <p className="text-[13px] text-[var(--color-muted)]">No calls in this window.</p>
              ) : (
                <table className="w-full text-left text-[12.5px]">
                  <thead className="text-[var(--color-muted)]">
                    <tr>
                      <th className="py-1.5 font-semibold">Call</th>
                      <th className="font-semibold">Playbook</th>
                      <th className="font-semibold">Mode</th>
                      <th className="font-semibold">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(s.recent_calls ?? []).map((c) => (
                      <tr key={String(c.call_id)} className="border-t border-[var(--color-line)]">
                        <td className="py-1.5">
                          <button type="button" className="font-mono underline-offset-2 hover:underline" onClick={() => setOpenCall(String(c.call_id))}>
                            {String(c.call_id).slice(0, 12)}
                          </button>
                        </td>
                        <td>{String(c.playbook ?? "")}</td>
                        <td>{String(c.mode ?? "")}</td>
                        <td>{String(c.status ?? "")}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </Card>
          </div>

          {openCall && <CallDrawer callId={openCall} onClose={() => setOpenCall(null)} />}
          <p className="mt-6 text-[11.5px] text-[var(--color-muted)]">
            As of {new Date(data.as_of_ms).toLocaleString()}
            {Object.keys(data.errors ?? {}).length > 0 && ` · ${Object.keys(data.errors ?? {}).length} section(s) unavailable`}
          </p>
        </>
      )}
    </section>
  );
}

function kpis(s: Record<string, Row[]>) {
  const calls = (s.calls_daily ?? []).reduce((a, r) => a + num(r.calls), 0);
  const minutes = (s.calls_daily ?? []).reduce((a, r) => a + num(r.calls) * num(r.avg_minutes), 0);
  const voice = (s.voice_daily ?? []).reduce((a, r) => a + num(r.voice_sessions), 0);
  const sent = s.sentiment_daily ?? [];
  const turns = sent.reduce((a, r) => a + num(r.customer_turns), 0);
  const sentiment = turns ? sent.reduce((a, r) => a + num(r.avg_sentiment) * num(r.customer_turns), 0) / turns : NaN;
  const lat = (s.latency ?? [])[0] ?? {};
  return {
    calls,
    voice,
    avgMin: calls ? minutes / calls : NaN,
    p50: num(lat.p50_ms),
    p90: num(lat.p90_ms),
    sentiment,
  };
}

function Tile({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="panel p-4">
      <div className="text-[11px] font-semibold uppercase tracking-wider text-[var(--color-muted)]">{label}</div>
      <div className="mt-1 font-mono text-[24px] font-semibold tabular-nums">{value}</div>
      {hint && <div className="mt-0.5 text-[11px] text-[var(--color-muted)]">{hint}</div>}
    </div>
  );
}

function Card({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="panel p-4">
      <h2 className="mb-3 text-[13px] font-semibold">{title}</h2>
      {children}
    </div>
  );
}

function Empty() {
  return <p className="py-6 text-center text-[13px] text-[var(--color-muted)]">No data in this window.</p>;
}

/** Small inline SVG bars. Scales to the largest value; no chart library. */
function BarChart({ rows, xKey, yKey }: { rows: Row[]; xKey: string; yKey: string }) {
  if (rows.length === 0) return <Empty />;
  const W = 520, H = 150, pad = 18;
  const max = Math.max(1, ...rows.map((r) => num(r[yKey])));
  const bw = (W - pad * 2) / rows.length;
  return (
    <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Bar chart" className="w-full">
      {rows.map((r, i) => {
        const h = ((H - pad * 2) * num(r[yKey])) / max;
        return (
          <g key={String(r[xKey])}>
            <rect x={pad + i * bw + 3} y={H - pad - h} width={Math.max(2, bw - 6)} height={h} rx={3} fill="var(--c-accent)" opacity={0.85}>
              <title>{`${String(r[xKey])}: ${num(r[yKey])}`}</title>
            </rect>
          </g>
        );
      })}
      <line x1={pad} x2={W - pad} y1={H - pad} y2={H - pad} stroke="var(--color-line)" />
    </svg>
  );
}

function LineChart({ rows, xKey, yKey, min, max }: { rows: Row[]; xKey: string; yKey: string; min: number; max: number }) {
  if (rows.length === 0) return <Empty />;
  const W = 520, H = 150, pad = 18;
  const x = (i: number) => (rows.length === 1 ? W / 2 : pad + (i * (W - pad * 2)) / (rows.length - 1));
  const y = (v: number) => H - pad - ((Math.min(max, Math.max(min, v)) - min) / (max - min)) * (H - pad * 2);
  const pts = rows.map((r, i) => `${x(i)},${y(num(r[yKey]))}`).join(" ");
  return (
    <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Line chart" className="w-full">
      <line x1={pad} x2={W - pad} y1={y(0)} y2={y(0)} stroke="var(--color-line)" strokeDasharray="3 4" />
      <polyline points={pts} fill="none" stroke="var(--c-accent)" strokeWidth={2} strokeLinejoin="round" />
      {rows.map((r, i) => (
        <circle key={String(r[xKey])} cx={x(i)} cy={y(num(r[yKey]))} r={3} fill="var(--c-accent)">
          <title>{`${String(r[xKey])}: ${fmt(num(r[yKey]), 2)}`}</title>
        </circle>
      ))}
    </svg>
  );
}

function BarList({ rows }: { rows: Row[] }) {
  if (rows.length === 0) return <Empty />;
  const max = Math.max(1, ...rows.map((r) => num(r.n)));
  return (
    <ul className="space-y-2">
      {rows.map((r) => (
        <li key={String(r.label)} className="text-[12.5px]">
          <div className="flex justify-between gap-2">
            <span className="truncate">{String(r.label)}</span>
            <span className="font-mono tabular-nums text-[var(--color-muted)]">{num(r.n)}</span>
          </div>
          <div className="mt-1 h-1.5 rounded-full bg-[var(--color-raised)]">
            <div className="h-1.5 rounded-full bg-[var(--c-accent)]" style={{ width: `${(num(r.n) / max) * 100}%` }} />
          </div>
        </li>
      ))}
    </ul>
  );
}

function Outcomes({ rows }: { rows: Row[] }) {
  if (rows.length === 0) return <Empty />;
  const kinds = [...new Set(rows.map((r) => String(r.kind)))];
  const statuses = ["accepted", "edited", "rejected", "shown"];
  return (
    <table className="w-full text-right text-[12.5px]">
      <thead className="text-[var(--color-muted)]">
        <tr>
          <th className="text-left font-semibold">Kind</th>
          {statuses.map((s) => (
            <th key={s} className="font-semibold capitalize">{s}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {kinds.map((kind) => (
          <tr key={kind} className="border-t border-[var(--color-line)]">
            <td className="py-1.5 text-left">{kind}</td>
            {statuses.map((st) => (
              <td key={st} className="font-mono tabular-nums">
                {num(rows.find((r) => r.kind === kind && r.status === st)?.n)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function CallDrawer({ callId, onClose }: { callId: string; onClose: () => void }) {
  const [detail, setDetail] = useState<CallDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    getJson<CallDetail>(`/api/admin/calls/${encodeURIComponent(callId)}`)
      .then((d) => alive && setDetail(d))
      .catch((e: Error) => alive && setError(e.message));
    return () => {
      alive = false;
    };
  }, [callId]);
  return (
    <aside aria-label="Call detail" className="panel mt-4 max-h-[70vh] overflow-auto p-4 scroll-thin">
      <div className="flex items-center justify-between">
        <h2 className="font-mono text-[13px]">{callId}</h2>
        <button type="button" className="btn btn-ghost py-1 text-[12px]" onClick={onClose}>Close</button>
      </div>
      {error && <p className="mt-3 text-[13px] text-[var(--c-neg)]">{error}</p>}
      {!detail && !error && <p className="mt-3 text-[13px] text-[var(--color-muted)]">Loading…</p>}
      {detail && (
        <div className="mt-3 grid gap-4 md:grid-cols-2">
          <div>
            <h3 className="kbd-label mb-2">Transcript</h3>
            <ol className="space-y-1.5 text-[13px]">
              {detail.utterances.map((u) => (
                <li key={String(u.seq)}>
                  <span className="font-semibold capitalize">{String(u.speaker)}: </span>
                  {String(u.text)}
                </li>
              ))}
            </ol>
          </div>
          <div className="space-y-4">
            <div>
              <h3 className="kbd-label mb-2">Cards</h3>
              <ul className="space-y-2 text-[13px]">
                {detail.cards.map((c) => (
                  <li key={String(c.card_id)} className="rounded-[var(--radius-control)] border border-[var(--color-line)] p-2">
                    <span className="chip chip-neutral mr-2">{String(c.kind)}</span>
                    {String(c.status)} · <span className="font-semibold">{String(c.title ?? "")}</span>
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <h3 className="kbd-label mb-2">Post-call summary</h3>
              {detail.summary ? (
                <p className="text-[13px] leading-relaxed">{String(detail.summary.summary ?? "")}</p>
              ) : (
                <p className="text-[13px] text-[var(--color-muted)]">Not scored yet (the scoring job runs every 30 minutes).</p>
              )}
            </div>
          </div>
        </div>
      )}
    </aside>
  );
}
