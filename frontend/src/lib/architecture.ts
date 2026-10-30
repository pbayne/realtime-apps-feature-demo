// Data for the "How it's built" explorer (#/admin/architecture). Every number is copied from docs/ARCHITECTURE.md
// (labels: MEASURED or ESTIMATED as there). Edit the doc first, then this file.

export interface Metric {
  label: string;
  value: string;
  /** MEASURED / ESTIMATED, as labelled in docs/ARCHITECTURE.md. */
  basis?: "MEASURED" | "ESTIMATED";
}

export interface ArchNode {
  id: string;
  stage: string;
  title: string;
  role: string;
  why: string;
  metrics: Metric[];
  doc: string;
  /** Only shown with the scale-path toggle on (Kafka + Spark Real-Time Mode). */
  scale?: boolean;
}

export interface Stage {
  id: string;
  label: string;
}

export const STAGES: Stage[] = [
  { id: "browser", label: "Browser" },
  { id: "app", label: "Databricks App" },
  { id: "serving", label: "Model Serving" },
  { id: "state", label: "Live state" },
  { id: "history", label: "History" },
  { id: "jobs", label: "Lakeflow Jobs" },
  { id: "knowledge", label: "Knowledge" },
  { id: "consume", label: "Consume" },
];

export const NODES: ArchNode[] = [
  {
    id: "browser",
    stage: "browser",
    title: "Browser console",
    role: "Captures the mic as 16 kHz PCM, plays replies, renders cards over a WebSocket.",
    why: "A thin client. No model keys reach the browser; the app is the only caller of Databricks services.",
    metrics: [{ label: "Audio path", value: "PCM16, 16 kHz, over WebSocket" }],
    doc: "docs/SPEC.md section 9.2a",
  },
  {
    id: "app",
    stage: "app",
    title: "Sidekick app (FastAPI)",
    role: "One process: speech orchestration, enrichment, perception, retrieval, decision, card composition, guardrails, WebSockets.",
    why: "Runs as its own service principal with every backend declared as an app resource. No servers to run.",
    metrics: [
      { label: "Final to enrichment", value: "about 1 ms, in process", basis: "MEASURED" },
      { label: "Customer final to card.start", value: "0.30 s p50 (fee objection)", basis: "MEASURED" },
      { label: "Customer final to card.done", value: "2.0 s p50 (fee objection)", basis: "MEASURED" },
    ],
    doc: "docs/ARCHITECTURE.md sections 1 and 3.2",
  },
  {
    id: "parakeet",
    stage: "serving",
    title: "Parakeet STT (GPU)",
    role: "Turns a rolling 12 s window of audio into the transcript.",
    why: "A custom model registered in Unity Catalog and served on serverless GPU; the endpoint can scale to zero.",
    metrics: [
      { label: "End of speech to final transcript", value: "0.84–0.92 s (includes 700 ms endpoint silence)", basis: "MEASURED" },
      { label: "Window decode round trip", value: "p50 312 ms from a laptop", basis: "MEASURED" },
    ],
    doc: "docs/ARCHITECTURE.md section 3.3",
  },
  {
    id: "claude",
    stage: "serving",
    title: "Claude (Foundation Model API)",
    role: "Haiku for perception and voice replies; Sonnet for card composition and post-call scoring.",
    why: "Pay-per-token models in the same workspace, under the same identity and resource grants as the rest of the app.",
    metrics: [
      { label: "Voice first token, practice prompt", value: "0.41 s", basis: "MEASURED" },
      { label: "Voice first token, assistant prompt (with retrieval)", value: "1.03 s", basis: "MEASURED" },
    ],
    doc: "docs/ARCHITECTURE.md section 3.3",
  },
  {
    id: "kokoro",
    stage: "serving",
    title: "Kokoro TTS (GPU)",
    role: "Speaks each reply sentence as soon as it is ready, so the first audio plays early.",
    why: "Served from Unity Catalog on GPU beside the app. Voice mode only.",
    metrics: [
      { label: "First audio per sentence", value: "107–144 ms", basis: "MEASURED" },
      { label: "Cold start from scale-to-zero", value: "112 s", basis: "MEASURED" },
    ],
    doc: "docs/ARCHITECTURE.md sections 3.3 and 5",
  },
  {
    id: "lakebase",
    stage: "state",
    title: "Lakebase (Postgres)",
    role: "Live state: calls, utterances, enrichments, cards, feedback, voice turns, user preferences.",
    why: "Managed Postgres. The app writes with a short-lived credential, and a NOTIFY trigger wakes waiters on enrichment.",
    metrics: [{ label: "Write plus NOTIFY round trip", value: "about 300 ms p50 (laptop to us-west-2; in region lower)", basis: "MEASURED" }],
    doc: "docs/ARCHITECTURE.md section 3.1",
  },
  {
    id: "zerobus",
    stage: "history",
    title: "Zerobus Ingest",
    role: "Streams every history record from the app into UC Delta, in the background, never on the card path.",
    why: "Direct ingest with no broker to run. Bounded queue, retries, and a drop counter that is visible.",
    metrics: [{ label: "Deployed run", value: "41 queued, 41 written, 0 dropped, 0 failed", basis: "MEASURED" }],
    doc: "docs/ARCHITECTURE.md sections 3.3 and 6",
  },
  {
    id: "delta",
    stage: "history",
    title: "Unity Catalog Delta",
    role: "History tables (calls, utterances, enrichments, cards, feedback, voice) and the gold tables analysts use.",
    why: "Governed tables with lineage and access control. The same data serves SQL, dashboards, Genie and the admin page.",
    metrics: [{ label: "Visible in Delta", value: "within seconds (estimated, not on the live path)", basis: "ESTIMATED" }],
    doc: "docs/ARCHITECTURE.md section 6",
  },
  {
    id: "reconcile",
    stage: "jobs",
    title: "Job: reconcile history",
    role: "Copies Lakebase into Delta every 15 minutes and removes Zerobus duplicates by natural key.",
    why: "A serverless scheduled job. Nothing to size or patch.",
    metrics: [{ label: "Schedule", value: "every 15 min, 48 h watermark" }],
    doc: "resources/job.yml",
  },
  {
    id: "scoring",
    stage: "jobs",
    title: "Job: post-call scoring",
    role: "ai_query scores finished calls: summary, methodology coverage, objections handled, overall score.",
    why: "The model runs inside SQL on governed data, so nothing is exported to score it.",
    metrics: [{ label: "Schedule", value: "every 30 min" }],
    doc: "src/notebooks/post_call_scoring.py",
  },
  {
    id: "insights",
    stage: "jobs",
    title: "Job: talk-track insights",
    role: "Daily ai_query over the last 7 days of transcripts and cards: recurring objections, better responses, KB gaps.",
    why: "Same governed tables in, a Delta gold table out. The admin page reads its latest rows.",
    metrics: [{ label: "Schedule", value: "daily, serverless" }],
    doc: "src/notebooks/talk_track_insights.py",
  },
  {
    id: "vs",
    stage: "knowledge",
    title: "Vector Search (opt-in KB)",
    role: "Retrieval index for large playbook knowledge bases, filtered by playbook.",
    why: "A managed index synced from a Delta table. The default is in-app embeddings, which suit a small KB.",
    metrics: [{ label: "Default", value: "in-app gte embeddings (auto)" }],
    doc: "src/notebooks/build_kb_index.py",
  },
  {
    id: "bi",
    stage: "consume",
    title: "AI/BI dashboards and Genie",
    role: "Enablement and leadership browse call analytics; Genie answers questions over the gold tables.",
    why: "Uses the same Unity Catalog permissions as every other table. No second copy of the data.",
    metrics: [],
    doc: "docs/SPEC.md section 13",
  },
  {
    id: "kafka",
    stage: "app",
    title: "Kafka (scale path)",
    role: "Utterances are produced with acks=all and read by the RTM job.",
    why: "A standard broker (Redpanda Serverless, Confluent or MSK), connected through a secret scope.",
    metrics: [{ label: "Produce, Redpanda Serverless", value: "about 100 ms p50 (laptop to us-west-2)", basis: "MEASURED" }],
    doc: "docs/ARCHITECTURE.md section 2",
    scale: true,
  },
  {
    id: "rtm",
    stage: "state",
    title: "Spark Real-Time Mode (scale path)",
    role: "Enriches utterances on a classic RTM cluster and upserts enrichments into Lakebase.",
    why: "Sub-second streaming with the native Postgres sink. The scale path, not the default.",
    metrics: [{ label: "Processing", value: "130–200 ms (estimated)", basis: "ESTIMATED" }],
    doc: "docs/ARCHITECTURE.md section 2",
    scale: true,
  },
];

export interface Step {
  title: string;
  text: string;
  /** Node ids lit for this step. */
  nodes: string[];
  timings: Metric[];
}

/** One utterance, end to end, with the measured timings (docs/ARCHITECTURE.md section 3.3 and 3.2). */
export const STEPS: Step[] = [
  {
    title: "You speak",
    text: "The browser streams 16 kHz PCM to the app over the audio socket.",
    nodes: ["browser"],
    timings: [],
  },
  {
    title: "Parakeet transcribes",
    text: "The rolling window is decoded and the final transcript is committed.",
    nodes: ["parakeet", "app"],
    timings: [{ label: "End of speech to final transcript", value: "0.84–0.92 s", basis: "MEASURED" }],
  },
  {
    title: "Enrich and store",
    text: "Enrichment runs in process. The utterance and its enrichment are written to Lakebase in the background.",
    nodes: ["app", "lakebase"],
    timings: [{ label: "Enrichment", value: "about 1 ms", basis: "MEASURED" }],
  },
  {
    title: "Perceive and retrieve",
    text: "Perception (Haiku) and knowledge retrieval run beside each other. The decision picks a card or stays quiet.",
    nodes: ["app", "claude"],
    timings: [{ label: "Customer final to card.start", value: "0.30 s p50", basis: "MEASURED" }],
  },
  {
    title: "Compose the card",
    text: "Sonnet writes the card, streamed to the screen while it is generated.",
    nodes: ["claude", "app", "browser"],
    timings: [{ label: "Customer final to card.done", value: "2.0 s p50", basis: "MEASURED" }],
  },
  {
    title: "Speak the reply (voice)",
    text: "In voice mode the reply is spoken sentence by sentence as it is generated.",
    nodes: ["claude", "kokoro", "browser"],
    timings: [
      { label: "Kokoro first audio per sentence", value: "107–144 ms", basis: "MEASURED" },
      { label: "Turn to first audible reply, practice", value: "0.60–0.69 s", basis: "MEASURED" },
      { label: "Turn to first audible reply, assistant", value: "1.29 s", basis: "MEASURED" },
    ],
  },
  {
    title: "Keep history",
    text: "The history record goes to Zerobus and lands in Delta. It is never on the card path.",
    nodes: ["zerobus", "delta"],
    timings: [{ label: "Visible in Delta", value: "within seconds", basis: "ESTIMATED" }],
  },
  {
    title: "Analyse later",
    text: "Reconcile runs every 15 minutes, scoring every 30, insights daily. Analysts and this page read the results.",
    nodes: ["reconcile", "scoring", "insights", "delta", "bi"],
    timings: [],
  },
];

export const NODE_BY_ID: Record<string, ArchNode> = Object.fromEntries(NODES.map((n) => [n.id, n]));
