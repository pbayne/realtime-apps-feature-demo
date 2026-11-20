// Account identification + identity-verification + account history (RAG) for the
// inbound-call co-pilot. Phase 1 ships a client-side mock dataset so the flow is
// visible in ?mock=1 with no backend. The wire shapes here mirror the planned
// `accounts` / `account_calls` tables (src/app/store, history/schema.py) so the
// backend swap is a fetch() in place of the mock lookups.

export type Rag = "green" | "yellow" | "red";

/** One past interaction with this account: a short synopsis + how the call went. */
export interface AccountCall {
  call_id: string;
  when_ms: number;
  channel: "voice" | "phone" | "chat";
  topic: string;
  /** One or two line synopsis of what happened. */
  synopsis: string;
  rag: Rag;
}

/** A customer account, keyed by the inbound phone + name used to identify them. */
export interface Account {
  account_id: string;
  first_name: string;
  last_name: string;
  phone: string;
  /** Masked for display; never show the full value pre-verification. */
  email: string;
  company?: string;
  tier: "Standard" | "Premium" | "Private";
  /** Identity challenge. In a real system `security_answer` would never leave the server. */
  security_question: string;
  security_answer: string;
  relationship_since: string;
  summary: string;
  past_calls: AccountCall[];
}

export interface LookupResult {
  match: Account | null;
  /** When a phone matches but the name does not, we flag a partial match for the rep. */
  partial: boolean;
}

const DAY = 24 * 60 * 60 * 1000;
const now = Date.now();

// ---- Mock directory (stands in for the Lakebase `accounts` table) ----------------
export const MOCK_ACCOUNTS: Account[] = [
  {
    account_id: "AC-10231",
    first_name: "Dana",
    last_name: "Whitfield",
    phone: "+1 (415) 555-0142",
    email: "d••••��@gmail.com",
    company: "Whitfield Family Trust",
    tier: "Private",
    security_question: "What city were you born in?",
    security_answer: "portland",
    relationship_since: "2016",
    summary:
      "Joint household with spouse Mike. Retirement-at-62 goal, ~$1.8M across brokerage, rollover IRA and a legacy 401k. Fee-sensitive; sold in the March 2020 drawdown and regretted it.",
    past_calls: [
      {
        call_id: "PC-8841",
        when_ms: now - 6 * DAY,
        channel: "voice",
        topic: "Fee review",
        synopsis:
          "Raised the advisory fee as a concern. Left leaning toward staying but wanted to talk to Mike. Open follow-up.",
        rag: "yellow",
      },
      {
        call_id: "PC-8702",
        when_ms: now - 41 * DAY,
        channel: "phone",
        topic: "401k rollover",
        synopsis: "Confirmed intent to roll the old employer 401k into the managed IRA. Paperwork sent and signed.",
        rag: "green",
      },
      {
        call_id: "PC-8533",
        when_ms: now - 118 * DAY,
        channel: "voice",
        topic: "Market volatility",
        synopsis: "Anxious about a down week, floated going to cash. Talked through the plan; no action taken.",
        rag: "red",
      },
    ],
  },
  {
    account_id: "AC-20774",
    first_name: "Marcus",
    last_name: "Alvarez",
    phone: "+1 (312) 555-0199",
    email: "m•••••@outlook.com",
    company: "Alvarez Consulting LLC",
    tier: "Premium",
    security_question: "What is the name of your first pet?",
    security_answer: "biscuit",
    relationship_since: "2021",
    summary:
      "Small-business owner, variable income. Building an emergency fund and a SEP-IRA. Responsive, prefers text follow-ups.",
    past_calls: [
      {
        call_id: "PC-9120",
        when_ms: now - 12 * DAY,
        channel: "chat",
        topic: "SEP-IRA setup",
        synopsis: "Walked through SEP-IRA contribution limits for the year. Positive, ready to fund.",
        rag: "green",
      },
      {
        call_id: "PC-9004",
        when_ms: now - 63 * DAY,
        channel: "phone",
        topic: "Cash-flow dip",
        synopsis: "Slow quarter, asked about pausing contributions. Agreed to a lower auto-contribution temporarily.",
        rag: "yellow",
      },
    ],
  },
  {
    account_id: "AC-30488",
    first_name: "Priya",
    last_name: "Nair",
    phone: "+1 (206) 555-0176",
    email: "p•••••@proton.me",
    tier: "Standard",
    security_question: "What street did you grow up on?",
    security_answer: "maple",
    relationship_since: "2023",
    summary:
      "Early-career, first advised account. Aggressive growth allocation, long horizon. Highly engaged, asks detailed questions.",
    past_calls: [
      {
        call_id: "PC-9330",
        when_ms: now - 3 * DAY,
        channel: "voice",
        topic: "Allocation review",
        synopsis: "Reviewed the growth tilt and ESG screens. Happy with the plan, no changes.",
        rag: "green",
      },
    ],
  },
];

/** Look an account up the way an inbound call would: by phone, cross-checked with the name. */
export function lookupAccount(phone: string, name: string): LookupResult {
  const digits = (s: string) => s.replace(/\D/g, "");
  const p = digits(phone);
  const n = name.trim().toLowerCase();
  if (!p && !n) return { match: null, partial: false };
  const byPhone = MOCK_ACCOUNTS.find((a) => p.length >= 4 && digits(a.phone).endsWith(p.slice(-10)));
  if (byPhone) {
    const full = `${byPhone.first_name} ${byPhone.last_name}`.toLowerCase();
    const nameOk = !n || full.includes(n) || n.includes(byPhone.last_name.toLowerCase());
    return { match: byPhone, partial: !nameOk };
  }
  const byName = n ? MOCK_ACCOUNTS.find((a) => `${a.first_name} ${a.last_name}`.toLowerCase().includes(n)) : undefined;
  return { match: byName ?? null, partial: false };
}

/** Verify the spoken/typed answer against the stored one (case- and space-insensitive). */
export function verifyAnswer(account: Account, answer: string): boolean {
  const norm = (s: string) => s.trim().toLowerCase().replace(/\s+/g, " ");
  return norm(answer) === norm(account.security_answer);
}

export const RAG_META: Record<Rag, { label: string; chip: string; dot: string }> = {
  green: { label: "Went well", chip: "chip-pos", dot: "var(--c-pos-base)" },
  yellow: { label: "Needs follow-up", chip: "chip-objection", dot: "var(--c-warn-base)" },
  red: { label: "At risk", chip: "chip-risk", dot: "var(--c-neg-base)" },
};

export function fmtAgo(ms: number): string {
  const d = Math.round((Date.now() - ms) / DAY);
  if (d <= 0) return "today";
  if (d === 1) return "yesterday";
  if (d < 30) return `${d} days ago`;
  const mo = Math.round(d / 30);
  return mo === 1 ? "1 month ago" : `${mo} months ago`;
}
