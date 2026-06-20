# AGENTS.project.md — Mandarin Learning Engine

## Purpose

A language-learning engine targeting Mandarin Chinese (simplified characters).
Covers vocabulary acquisition, comprehensible-input sentence generation, learner
modelling, distributed content sharing, and (later) audio generation.

## Project Phases

| Phase | Title | Status |
|-------|-------|--------|
| 01 | Core Data Architecture | IN PROGRESS |
| 02 | Vocabulary Engine | PLANNED |
| 03 | Sentence Engine | PLANNED |
| 04 | Learner Fit Engine | PLANNED |
| 05 | Distributed Content System | PLANNED |
| 06 | Review Workflow | PLANNED |
| 07 | Audio Engine | PLANNED |

## Module Layout

All Phase 01 code lives in a single file: `mandarin_learning_engine.py`.
As phases grow, sub-modules will be extracted and listed here.

## Dev Style

- **CLI-args style** (not REPL-driven). Run the engine directly:
  ```
  & "<venv>\Scripts\python.exe" mandarin_learning_engine.py [--data-dir PATH] [--learner-id ID]
  ```
- Data files (SQLite databases, patches) live under `data/` — gitignored.
- No absolute paths in source. Data directory is passed at runtime or defaults to `data/`.

## Databases

| File | Purpose |
|------|---------|
| `data/generation_cache.sqlite` | Raw corpus, AI generations, validation history |
| `data/common_content.sqlite` | Lexemes, rankings, sentence candidates |
| `data/learner_<id>.sqlite` | Per-learner knowledge estimates, review schedule |
| `data/patches/` | Immutable JSON patch files + manifest |

## Key Classes (Phase 01)

- `CorpusCache` — source & revision tracking, AI generation history
- `CorpusIngestor` — corpus/frequency imports, sentence extraction
- `LexemeRepository` — lexeme CRUD, rankings, token resolution
- `SentenceAnalyzer` — candidate storage, analysis, scoring, approval
- `LearnerModel` — knowledge estimates, SM-2 review scheduling
- `LearnerSentenceFitScorer` — known-word %, inference score, CI+1
- `PatchManager` — immutable patches, manifest, idempotent apply
- `MandarinLearningEngine` — top-level facade

## No sibling-repo dependencies in Phase 01

This repo has no `versholn` or cross-repo dependencies in Phase 01.
All imports are stdlib only at module level.
