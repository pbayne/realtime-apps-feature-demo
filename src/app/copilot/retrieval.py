"""KB retrieval over playbook markdown, chunked by "## " section.

Backends (Hit.backend):
  embed         in-memory cosine over gte-large embeddings (EMBED_MODEL), when the LLM is up.
  keyword       idf-weighted token overlap, offline fallback. Scores are in [0, 1].
  vector_search Databricks Vector Search (VectorSearchStub). Stub: wired but untested locally.

Retrieval starts as soon as the customer's final arrives (session.py), and speculatively on stable
customer partials; keyword_only is the budget fallback. A failed embedding call degrades that one query to keyword. Thresholds differ per
backend (settings.retrieval_min_score_*), so a Hit.score is only comparable within one backend.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
from collections import Counter
from dataclasses import dataclass
from typing import Optional

from config import Settings
from llm import LlmClient, LlmUnavailable

from copilot.playbooks import KbDoc, Playbook

log = logging.getLogger("copilot.retrieval")

_STOP = frozenset(
    "a an the and or of to in on for with is are was were be been it this that these those i you we they he she "
    "my your our their me us them do does did so if at as by from not no yes can could would should will just "
    "about there here what when how why who which".split()
)
_TOKEN = re.compile(r"[a-z0-9]+")
_EMBED_CHARS = 1500


_SUFFIXES = ("ing", "ed", "es", "s")


def _stem(t: str) -> str:
    """Light suffix stripping so 'charge', 'charged', 'charges' share a token."""
    for suf in _SUFFIXES:
        if len(t) > len(suf) + 3 and t.endswith(suf):
            return t[: -len(suf)]
    return t


def tokens(text: str) -> list[str]:
    out = []
    for t in _TOKEN.findall(text.lower()):
        if t in _STOP or len(t) < 2:
            continue
        out.append(_stem(t))
    return out


@dataclass(frozen=True)
class Hit:
    doc_id: str
    title: str
    section: str
    text: str
    score: float
    backend: str


@dataclass
class _Chunk:
    doc_id: str
    title: str
    section: str
    text: str
    tokens: Counter
    vec: Optional[list[float]] = None


@dataclass
class _Index:
    chunks: list[_Chunk]
    idf: dict[str, float]
    unseen_idf: float = 1.0
    embedded: bool = False


def chunk_doc(doc: KbDoc) -> list[_Chunk]:
    parts = re.split(r"^##\s+(.+)$", doc.text, flags=re.MULTILINE)
    out: list[_Chunk] = []
    intro = parts[0].strip()
    body_pairs = [("Overview", intro)] if intro and not intro.startswith("# ") else []
    for i in range(1, len(parts), 2):
        body_pairs.append((parts[i].strip(), parts[i + 1].strip()))
    for section, body in body_pairs:
        text = re.sub(r"^#\s+.+$", "", body, flags=re.MULTILINE).strip()
        if not text:
            continue
        out.append(_Chunk(doc.doc_id, doc.title, section, text, Counter(tokens(f"{doc.title} {section} {text}"))))
    return out


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


class VectorSearchStub:
    """Databricks Vector Search client for databricks mode.

    Wired to databricks-sdk `vector_search_indexes.query_index`. Not exercised
    against a live index yet (the index is built by src/notebooks/build_kb_index.py).
    Any error makes Retriever fall back to the in-memory backends.
    """

    def __init__(self, settings: Settings) -> None:
        self._s = settings

    async def search(self, playbook: str, query: str, k: int) -> list[Hit]:
        from databricks.sdk import WorkspaceClient

        index_name = self._s.vs_index_template.format(playbook=playbook)

        def _call():
            w = WorkspaceClient()
            return w.vector_search_indexes.query_index(
                index_name=index_name,
                columns=["doc_id", "title", "section", "text"],
                query_text=query,
                num_results=k,
            )

        resp = await asyncio.to_thread(_call)
        rows = (resp.result.data_array if resp.result else None) or []
        hits = []
        for row in rows:
            doc_id, title, section, text, score = (list(row) + [None] * 5)[:5]
            hits.append(Hit(doc_id or "", title or "", section or "", text or "", float(score or 0.0), "vector_search"))
        return hits


class Retriever:
    def __init__(self, llm: LlmClient, settings: Settings, vector_search: Optional[VectorSearchStub] = None,
                 backend: Optional[str] = None) -> None:
        self._llm = llm
        self._s = settings
        self._vs = vector_search
        self._backend = backend or settings.retrieval_backend
        self._indexes: dict[str, _Index] = {}
        self._lock = asyncio.Lock()
        self.last_backend: Optional[str] = None

    async def warm(self, playbooks: list[Playbook]) -> None:
        for pb in playbooks:
            try:
                await self._index(pb)
            except Exception:  # noqa: BLE001 - warm-up is best-effort
                log.exception("KB warm-up failed for %s", pb.name)

    def min_score(self, backend: str) -> float:
        return self._s.retrieval_min_score_keyword if backend == "keyword" else self._s.retrieval_min_score_embed

    async def _index(self, pb: Playbook) -> _Index:
        if pb.name in self._indexes:
            return self._indexes[pb.name]
        async with self._lock:
            if pb.name in self._indexes:
                return self._indexes[pb.name]
            chunks = [c for d in pb.kb_docs for c in chunk_doc(d)]
            df: Counter = Counter()
            for c in chunks:
                df.update(set(c.tokens))
            n = max(len(chunks), 1)
            idf = {t: math.log((n + 1) / (f + 0.5)) + 1.0 for t, f in df.items()}
            # Query tokens unseen in the KB are rare by definition: weight them at the maximum idf.
            index = _Index(chunks=chunks, idf=idf, unseen_idf=math.log(n + 1) + 1.0)
            self._indexes[pb.name] = index
            await self._embed_index(index)
            return index

    async def _embed_index(self, index: _Index) -> None:
        if self._backend == "keyword" or not self._llm.available or not index.chunks:
            return
        texts = [f"{c.title} / {c.section}: {c.text}"[:_EMBED_CHARS] for c in index.chunks]
        try:
            vecs = await self._llm.embed(self._llm.embed_model, texts)
            for c, v in zip(index.chunks, vecs):
                c.vec = v
            index.embedded = True
        except LlmUnavailable as exc:
            log.warning("embedding index failed; keyword retrieval only: %s", exc)

    def _keyword(self, index: _Index, query: str, k: int) -> list[Hit]:
        q = set(tokens(query))
        weight = {t: index.idf.get(t, index.unseen_idf) for t in q}
        total = sum(weight.values())
        scored = []
        for c in index.chunks:
            if not total:
                break
            got = sum(w for t, w in weight.items() if t in c.tokens)
            scored.append((got / total, c))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [Hit(c.doc_id, c.title, c.section, c.text, round(s, 4), "keyword") for s, c in scored[:k]]

    async def _embedded(self, index: _Index, query: str, k: int) -> list[Hit]:
        if not index.embedded:
            raise LlmUnavailable("index not embedded")
        (qv,) = await self._llm.embed(self._llm.embed_model, [query])
        scored = [(_cosine(qv, c.vec), c) for c in index.chunks if c.vec is not None]
        scored.sort(key=lambda x: x[0], reverse=True)
        return [Hit(c.doc_id, c.title, c.section, c.text, round(s, 4), "embed") for s, c in scored[:k]]

    async def search(self, pb: Playbook, query: str, k: Optional[int] = None, *, keyword_only: bool = False) -> list[Hit]:
        """Top-k KB hits. keyword_only skips every network call (used when the embedding path misses
        the turn's retrieval budget, so the decision never waits on a model round trip)."""
        k = k or self._s.retrieval_top_k
        backend = self._backend
        if keyword_only:
            index = await self._index(pb)
            self.last_backend = "keyword"
            return self._keyword(index, query, k)
        if backend == "vector_search" and self._vs is not None:
            try:
                hits = await asyncio.wait_for(self._vs.search(pb.name, query, k), timeout=1.0)
                self.last_backend = "vector_search"
                return hits
            except Exception as exc:  # noqa: BLE001 - fall through to in-memory
                log.warning("vector search failed, using in-memory retrieval: %s", exc)
        index = await self._index(pb)
        if backend != "keyword" and index.embedded and self._llm.available:
            try:
                hits = await asyncio.wait_for(self._embedded(index, query, k), timeout=2.0)
                self.last_backend = "embed"
                return hits
            except (LlmUnavailable, asyncio.TimeoutError) as exc:
                log.info("query embedding unavailable (%s); keyword retrieval for this turn", exc)
        self.last_backend = "keyword"
        return self._keyword(index, query, k)
