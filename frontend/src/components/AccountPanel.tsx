import { useState } from "react";
import { BadgeCheck, Building2, Phone, Search, ShieldCheck, ShieldQuestion, Sparkles, User, X, Clock } from "lucide-react";
import {
  fmtAgo,
  lookupAccount,
  RAG_META,
  verifyAnswer,
  type Account,
  type AccountCall,
} from "../lib/account";

type Stage = "lookup" | "verify" | "verified";

/**
 * Inbound-caller workflow for the console: identify the account by phone + name,
 * run an identity challenge (security question), then reveal the account detail and
 * RAG-scored call history. Phase 1 is mock-backed (lib/account.ts); the backend swap
 * is replacing lookupAccount/verifyAnswer with fetch() calls.
 */
export function AccountPanel({ className = "", mock = false }: { className?: string; mock?: boolean }) {
  const [stage, setStage] = useState<Stage>("lookup");
  const [account, setAccount] = useState<Account | null>(null);

  return (
    <section className={`panel flex max-h-[520px] min-h-[360px] flex-col overflow-hidden ${className}`}>
      <header className="flex items-center justify-between border-b border-[var(--color-line)] px-4 py-3">
        <div>
          <h2 className="flex items-center gap-2 text-[14px] font-semibold">
            <BadgeCheck className="size-4 text-[var(--c-info)]" /> Account
          </h2>
          <p className="text-[11.5px] text-[var(--color-muted)]">
            {stage === "lookup"
              ? "Identify the caller to pull their history"
              : account
                ? `${account.first_name} ${account.last_name} · ${account.account_id}`
                : ""}
          </p>
        </div>
        {stage !== "lookup" && (
          <button
            type="button"
            onClick={() => {
              setStage("lookup");
              setAccount(null);
            }}
            className="text-[11.5px] text-[var(--color-muted)] hover:text-slate-200"
          >
            New lookup
          </button>
        )}
      </header>

      <div className="scroll-thin min-h-0 flex-1 overflow-y-auto px-3.5 py-3.5">
        {stage === "lookup" && (
          <LookupForm
            mock={mock}
            onFound={(a) => {
              setAccount(a);
              setStage("verify");
            }}
          />
        )}
        {stage === "verify" && account && (
          <VerifyCard account={account} mock={mock} onVerified={() => setStage("verified")} onReject={() => setStage("lookup")} />
        )}
        {stage === "verified" && account && <AccountDetail account={account} />}
      </div>
    </section>
  );
}

function LookupForm({ mock, onFound }: { mock: boolean; onFound(a: Account): void }) {
  const [phone, setPhone] = useState("");
  const [name, setName] = useState("");
  const [note, setNote] = useState<string | null>(null);

  const run = () => {
    const { match, partial } = lookupAccount(phone, name);
    if (!match) {
      setNote("No account found for that phone or name. Check the number, or search by name only.");
      return;
    }
    if (partial) {
      setNote(`Phone matches ${match.first_name} ${match.last_name}, but the name didn't. Verify carefully.`);
    }
    onFound(match);
  };

  return (
    <div className="space-y-3">
      <Field icon={<Phone className="size-3.5" />} label="Caller phone">
        <input
          value={phone}
          onChange={(e) => setPhone(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && run()}
          placeholder="(415) 555-0142"
          className="w-full rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)] px-3 py-2 text-[13.5px] focus:border-[var(--c-info-base)]"
        />
      </Field>
      <Field icon={<User className="size-3.5" />} label="Name">
        <input
          value={name}
          onChange={(e) => setName(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && run()}
          placeholder="Dana Whitfield"
          className="w-full rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)] px-3 py-2 text-[13.5px] focus:border-[var(--c-info-base)]"
        />
      </Field>
      {note && (
        <p role="status" className="rounded-lg border border-[var(--color-line)] bg-[color-mix(in_srgb,var(--c-warn-base)_8%,transparent)] px-3 py-2 text-[12px] text-[var(--color-muted)]">
          {note}
        </p>
      )}
      <button type="button" onClick={run} className="btn btn-primary w-full py-2.5 text-[14px]">
        <Search className="size-4" /> Look up account
      </button>
      {mock && (
        <p className="text-center text-[11px] text-[var(--color-muted)]">
          Try (415) 555-0142 · (312) 555-0199 · (206) 555-0176
        </p>
      )}
    </div>
  );
}

function VerifyCard({
  account,
  mock,
  onVerified,
  onReject,
}: {
  account: Account;
  mock: boolean;
  onVerified(): void;
  onReject(): void;
}) {
  const [answer, setAnswer] = useState("");
  const [failed, setFailed] = useState(false);

  const check = () => {
    if (verifyAnswer(account, answer)) onVerified();
    else setFailed(true);
  };

  return (
    <article className="anim-card-in rounded-xl border border-[var(--c-info-base)]/40 bg-[var(--color-raised)] p-3.5">
      <div className="flex items-center gap-2">
        <span className="grid size-8 place-items-center rounded-lg" style={{ background: "color-mix(in srgb, var(--c-info-base) 16%, transparent)", color: "var(--c-info)" }}>
          <ShieldQuestion className="size-4" />
        </span>
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.09em]" style={{ color: "var(--c-info)" }}>
            Identity check
          </p>
          <h3 className="text-[14px] font-semibold leading-snug text-white">Verify before sharing details</h3>
        </div>
      </div>

      <p className="mt-3 text-[13px] leading-relaxed text-slate-200">
        Ask the caller:{" "}
        <span className="font-semibold text-white">{account.security_question}</span>
      </p>

      <input
        value={answer}
        onChange={(e) => {
          setAnswer(e.target.value);
          setFailed(false);
        }}
        onKeyDown={(e) => e.key === "Enter" && check()}
        placeholder="Type their answer…"
        aria-label="Caller's security answer"
        aria-invalid={failed || undefined}
        aria-describedby={failed ? "account-verify-error" : undefined}
        className="mt-3 w-full rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)] px-3 py-2 text-[13.5px] focus:border-[var(--c-info-base)]"
        autoFocus
      />
      {failed && (
        <p id="account-verify-error" role="alert" className="mt-2 text-[12px] text-rose-300">That doesn't match. Confirm the spelling, or ask a second time.</p>
      )}
      {mock && (
        <p className="mt-2 flex items-center gap-1.5 text-[11px] text-[var(--color-muted)]">
          <Sparkles className="size-3 shrink-0" style={{ color: "var(--c-info)" }} />
          <span>
            Demo answer: <span className="font-mono text-slate-300">{account.security_answer}</span> · shown in mock mode only
          </span>
        </p>
      )}

      <div className="mt-3 grid grid-cols-2 gap-2">
        <button type="button" onClick={check} className="btn border border-emerald-400/30 bg-emerald-400/10 py-2 text-emerald-200 hover:bg-emerald-400/20">
          <ShieldCheck className="size-3.5" /> Verify
        </button>
        <button type="button" onClick={onReject} className="btn btn-ghost py-2 text-[var(--color-muted)]">
          <X className="size-3.5" /> Can't verify
        </button>
      </div>
      <p className="mt-2.5 text-[11px] text-[var(--color-muted)]">
        Account history stays hidden until the caller's identity is confirmed.
      </p>
    </article>
  );
}

function AccountDetail({ account }: { account: Account }) {
  return (
    <div className="space-y-3.5">
      <div className="flex items-start gap-3 rounded-xl border border-emerald-400/25 bg-emerald-400/[0.06] px-3.5 py-3">
        <ShieldCheck className="mt-0.5 size-4 shrink-0 text-emerald-300" />
        <div className="min-w-0">
          <p className="text-[12.5px] font-semibold text-emerald-200">Identity verified</p>
          <p className="text-[12px] text-[var(--color-muted)]">You can discuss account details with this caller.</p>
        </div>
      </div>

      <div className="rounded-xl border border-[var(--color-line)] bg-[var(--color-raised)] p-3.5">
        <div className="flex items-center justify-between">
          <h3 className="text-[15px] font-semibold text-white">
            {account.first_name} {account.last_name}
          </h3>
          <span className="chip chip-intent">{account.tier}</span>
        </div>
        <dl className="mt-2.5 space-y-1.5 text-[12.5px]">
          <Row icon={<Phone className="size-3.5" />} value={account.phone} />
          <Row icon={<span className="text-[11px] font-mono">@</span>} value={account.email} />
          {account.company && <Row icon={<Building2 className="size-3.5" />} value={account.company} />}
          <Row icon={<Clock className="size-3.5" />} value={`Client since ${account.relationship_since}`} />
        </dl>
        <p className="mt-3 border-t border-[var(--color-line)] pt-3 text-[12.5px] leading-relaxed text-slate-300">
          {account.summary}
        </p>
      </div>

      <div>
        <p className="kbd-label mb-2">Past calls</p>
        <ul className="space-y-2">
          {account.past_calls.map((c) => (
            <PastCallRow key={c.call_id} call={c} />
          ))}
        </ul>
      </div>
    </div>
  );
}

function PastCallRow({ call }: { call: AccountCall }) {
  const rag = RAG_META[call.rag];
  return (
    <li className="relative overflow-hidden rounded-xl border border-[var(--color-line)] bg-[var(--color-raised)] p-3">
      <span className="absolute inset-y-0 left-0 w-[3px]" style={{ background: rag.dot }} aria-hidden />
      <div className="flex items-center justify-between gap-2">
        <span className="text-[13px] font-semibold text-slate-100">{call.topic}</span>
        <span className={`chip ${rag.chip} shrink-0`}>{rag.label}</span>
      </div>
      <p className="mt-1 text-[12.5px] leading-relaxed text-slate-300">{call.synopsis}</p>
      <div className="mt-1.5 flex items-center gap-2 text-[11px] text-[var(--color-muted)]">
        <span className="capitalize">{call.channel}</span>
        <span aria-hidden>·</span>
        <span>{fmtAgo(call.when_ms)}</span>
      </div>
    </li>
  );
}

function Field({ icon, label, children }: { icon: React.ReactNode; label: string; children: React.ReactNode }) {
  return (
    <label className="block">
      <span className="kbd-label mb-1.5 flex items-center gap-1.5">
        <span className="text-[var(--color-muted)]">{icon}</span>
        {label}
      </span>
      {children}
    </label>
  );
}

function Row({ icon, value }: { icon: React.ReactNode; value: string }) {
  return (
    <div className="flex items-center gap-2 text-slate-300">
      <span className="grid size-4 place-items-center text-[var(--color-muted)]">{icon}</span>
      <span className="min-w-0 truncate">{value}</span>
    </div>
  );
}
