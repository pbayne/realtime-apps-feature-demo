"""Voice mode (SPEC section 9): two-way speech with Databricks-hosted models only.

tts.py       Kokoro SSE client (PCM chunks, cancellable, closes its stream)
sentences.py speakable text and the streaming sentence chunker
items.py     the <<ITEMS>> delimiter splitter and tolerant item parsing (SPEC 9.4)
prompts.py   per-role system prompts and the context they are built from
session.py   VoiceSession: turns, interrupts, ordered per-sentence TTS, practice call wiring
"""
