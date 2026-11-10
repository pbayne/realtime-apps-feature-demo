# Sidekick demo: wealth advisor role play (about 12 minutes)

**Setup:** open the app, pick the **Wealth advisor discovery coach** playbook, source **Live mic**, then **Start call**. You play the **advisor**; a colleague (or you, reading both parts) plays the **client**, Dana. One mic hears both of you, so use the **Mic channel** toggle at the top of the console to say who is talking: click **agent** before your lines and **customer** before Dana's. Sidekick listens; it does not talk back during a live call.

---

## 1. The hook (1 min)

> "An advisor's first discovery meeting decides whether a relationship starts at all. It has to cover goals, timelines, risk, assets and fees, handle objections without pressure, and stay inside compliance, all live. Sidekick listens to the meeting as it happens, tracks what has been covered, reads the client's tone, and tells the advisor what to ask, what to say and what to avoid. It runs entirely on Databricks."

## 2. The role play (5 min)

Read it at a natural pace. Pause a beat after each line so the audience can see the panels update.

| # | Speaker | Line | What to point at |
|---|---|---|---|
| 1 | Dana | "My husband and I want to retire in about ten years, and we'd like to help pay for our daughter's college." | Live transcript. Discovery checklist ticks **Goals**, **Time horizon**, **Decision makers**. |
| 2 | Advisor | "That's helpful. Roughly what do you have today across your 401k, IRAs and savings?" | Checklist ticks **Assets**. Suggested **next question** for what is still missing. |
| 3 | Dana | "About one point two million. But honestly, what do you charge? One percent of my assets sounds too expensive." | **Fee objection** card with the firm's approved fee response from the knowledge base. |
| 4 | Dana | "And the market looks really high right now. Shouldn't we wait for a correction before investing?" | **Market timing** card: time in the market, phased entry. |
| 5 | Advisor | "Don't worry, with our growth model you can't lose. It's a sure thing." | **Compliance: no guarantees** card: "Do not guarantee returns… past performance does not predict future results." |
| 6 | Dana | "I already have an advisor, and frankly I'm frustrated. Nobody has ever explained the fees to me." | **Sentiment** drops to tense. **Existing advisor** card. |
| 7 | Advisor | "That's fair, and results do depend on markets. Let me show you exactly what you'd pay on your numbers. How much risk are you comfortable with if the portfolio dropped twenty percent in a bad year?" | Checklist ticks **Risk tolerance**. Sentiment steadies. |
| 8 | Dana | "I'd want to discuss it with my husband first. Can you send me some information?" | **Think-about-it** card: propose a joint meeting instead of a brochure. |
| 9 | Advisor | "Of course. Let's set a follow-up with both of you next Tuesday at 2pm, and I'll email a summary before then." | Next step captured. |
| 10 | Dana | "That sounds great, thank you." | Sentiment recovers toward positive on the chart. |

Talking points while it runs:
- "Every word is transcribed by **Parakeet on a Databricks GPU**, in under a second, inside the firm's workspace."
- "The cards come from the firm's own playbook and approved language, not generic advice."
- On line 5: "This is the one compliance teams care about. It catches the guarantee *during* the meeting, not in a review weeks later."
- The **discovery checklist** shows coverage: "A new advisor runs a complete discovery from day one."
- **Accept** or **dismiss** a card: "Advisor feedback is captured and tunes which cards show."

Then **End call**: the **post-call summary** appears with what was covered, what was promised, compliance flags and follow-ups.

## 3. It does the follow-up, with a human in the loop (2 min)

Switch source to **Talk**, role **assistant**, and say:

> *"Please email Dana a short summary of today's meeting, book a thirty minute follow-up with Dana and her husband next Tuesday at 2pm Eastern, and enroll her in the Retirement income review."*

- Cards appear in **Actions**: the email, the meeting and the enrollment. **Nothing runs until the advisor approves it.** Edit a field, then **Approve**. Show **Download .ics**.
- The meeting carries a **reminder the day before** and an alert ten minutes before.
- Point at the **Test mode** line: "In this demo every message is locked to my own inbox. That is how you would roll it out at a regulated firm: outbox mode first, supervisor review, then lift the lock when compliance signs off."
- Enrollments and contact updates can be **undone**.

## 4. How it's built (2 min): gear icon, then **How it's built**

Press **Play the flow** and walk the steps:

| Step | Databricks | Say |
|---|---|---|
| Browser → App | **Databricks Apps** | "One governed app; advisors sign in with their workspace identity." |
| Transcribe, speak | **Model Serving (GPU)**: Parakeet STT, Kokoro TTS | "Open models the firm hosts itself. Client audio never leaves the workspace." |
| Reason | **Foundation Model API** (Claude) | "Pay-per-token, governed and logged through the AI Gateway." |
| Live state | **Lakebase** (Postgres) | "Millisecond reads and writes while the meeting is live." |
| History | **Zerobus Ingest → Unity Catalog Delta** | "Every utterance, card and decision lands in Delta within seconds, with lineage and access control: the supervision record." |
| Jobs | **Lakeflow jobs**: reconcile, post-call scoring, talk-track insights | "Serverless, scheduled." |
| Knowledge | **Vector Search** over the playbooks | "Grounded in the firm's approved material and disclosures." |
| Consume | **AI/BI dashboards and Genie** | "Branch managers and compliance ask questions of the meetings in plain English." |

Toggle **Show scale path (Kafka + RTM)**: "For a national advisor network the same design swaps in Kafka and Spark Real-Time Mode. Same app, same tables."

## 5. Close the loop: Insights (1 min): **Insights** tab

- Meetings, client sentiment per day, top objections (fees leads), risk flags (unsupported claims).
- **Talk-track gaps**: written by a job over the Delta history, for example *"Costs layered on top of 1% fee: every fee objection ended with 'are there other fees?'"* and *"Fee quoted before any goal discovery."* "This is how the firm's talk tracks get better every quarter, from real meetings."
- Open a recent meeting: transcript, cards and the post-call summary, which flagged an advisor guaranteeing market-beating returns.

## 6. Close (30 s)

> "Live coaching in every client meeting, compliance caught in the moment, follow-ups done with the advisor in control, and every meeting improving the playbook, all governed in one platform. Where would this help first: ramping new advisors, or supervision and compliance review?"

---

### If something goes wrong
- **No transcript on Live mic**: allow the microphone in the browser's site settings and reload. If it is still slow, the GPU may be cold: run `sidekick warm`, or switch the source to **Replay** and play a scripted advisor meeting, which shows the same cards with no microphone.
- **Speakers mislabelled**: flip the channel switch in the header before each speaker's line.
- **Want a rehearsal partner?** Source **Talk**, role **practice customer**: Sidekick plays the client so an advisor can practice the fee objection.

After the demo: `sidekick off` (the GPUs bill until you do).
