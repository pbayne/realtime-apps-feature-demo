"""Compose the card body with the smart model, streamed (SPEC section 4 step 4).

Limits: title <= title_max_words (candidate title is used as-is, truncated), body <=
compose_max_words, enforced while streaming (the stream is cut at the cap). Compliance
and rules-only paths emit the template text in one delta; they never call a model.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from config import Settings
from llm import LlmClient, LlmUnavailable

from copilot.decide import Candidate
from copilot.playbooks import Playbook

log = logging.getLogger("copilot.compose")

DEFAULT_SYSTEM = (
    "You write one short live-coaching card for a person on a call. Reply with plain text only: "
    "no preamble, no quotes, no markdown. Use only the reference text for facts; if the reference "
    "does not cover it, say you will confirm. Be concise and natural."
)


_QUOTES = " \"'\u201c\u201d"


def clean_body(text: str) -> str:
    """Models sometimes wrap the card in quotes or add a label. Strip both."""
    text = text.strip().strip(_QUOTES).strip()
    for prefix in ("Card:", "Body:", "Coach:"):
        if text.startswith(prefix):
            text = text[len(prefix):].strip().strip(_QUOTES).strip()
    return text


def truncate_words(text: str, n: int) -> str:
    words = text.split()
    return " ".join(words[:n])


def template_text(hint: str, n: int) -> str:
    """Flatten KB markdown (bullets, headings) into one sentence for rules-only bodies."""
    lines = []
    for raw in hint.splitlines():
        line = raw.strip().lstrip("-*\u2022").strip()
        if line and not line.startswith("#"):
            lines.append(line)
    return truncate_words(" ".join(lines), n)


@dataclass
class ComposeResult:
    body: str
    mode: str               # "llm" | "template" | "verbatim"
    error: Optional[str] = None


async def compose_body(
    llm: LlmClient,
    settings: Settings,
    pb: Playbook,
    cand: Candidate,
    *,
    customer_text: str,
    recent: list[str],
    reference: str,
    emit: Callable[[str], Awaitable[None]],
) -> ComposeResult:
    if not cand.compose:
        body = truncate_words(cand.hint, settings.compose_max_words)
        await emit(body)
        return ComposeResult(body=body, mode="verbatim")
    if not llm.available:
        body = template_text(cand.hint, settings.compose_max_words)
        await emit(body)
        return ComposeResult(body=body, mode="template", error=llm.last_error)

    system = pb.prompts.get("compose") or DEFAULT_SYSTEM
    history = "\n".join(f"- {t}" for t in recent[-6:]) or "(none)"
    user = (
        f"Card type: {cand.kind.replace('_', ' ')}.\n"
        f"Coach note: {cand.hint}\n\n"
        f"Reference ({cand.source_doc or 'none'} / {cand.source_clause or 'none'}):\n{reference[:1500] or '(none)'}\n\n"
        f"Recent turns:\n{history}\n\n"
        f"Customer's latest turn: {customer_text[:400]}\n\n"
        "Address the advisor or agent directly, in one or two sentences, starting with a verb such as "
        "'Ask', 'Say', 'Confirm' or 'Acknowledge'. "
        f"Write at most {settings.compose_max_words} words."
    )
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    emitted: list[str] = []
    try:
        async for delta in llm.chat_stream(llm.smart_model, messages, max_tokens=120):
            joined = "".join(emitted) + delta
            if len(joined.split()) > settings.compose_max_words:
                # Cut at the word cap and stop streaming.
                remaining = truncate_words(joined, settings.compose_max_words)
                tail = remaining[len("".join(emitted)):]
                if tail:
                    emitted.append(tail)
                    await emit(tail)
                break
            emitted.append(delta)
            await emit(delta)
    except LlmUnavailable as exc:
        log.warning("compose stream failed (%s); template body", exc)
        body = "".join(emitted).strip() or template_text(cand.hint, settings.compose_max_words)
        if not "".join(emitted).strip():
            await emit(body)
        return ComposeResult(body=body, mode="template", error=str(exc)[:200])

    body = truncate_words(clean_body("".join(emitted)), settings.compose_max_words)
    if not body:
        body = template_text(cand.hint, settings.compose_max_words)
        await emit(body)
        return ComposeResult(body=body, mode="template", error="empty model output")
    return ComposeResult(body=body, mode="llm")
