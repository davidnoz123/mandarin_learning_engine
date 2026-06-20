# Mandarin Learning Engine - Project Snapshot v01

Version: 01
Status: Phase 01 Active

---

# Project Vision

Build a language-learning engine that can eventually support any language.

Initial target:

- Mandarin Chinese
- Simplified Characters

Primary objectives:

1. Vocabulary acquisition
2. Comprehensible-input sentence generation
3. Learner modelling
4. Incremental content improvement
5. Distributed content sharing
6. Audio generation (later phase)

---

# Project Phases

| Phase | Title | Status |
|---------|---------|---------|
| 01 | Core Data Architecture | IN PROGRESS |
| 02 | Vocabulary Engine | PLANNED |
| 03 | Sentence Engine | PLANNED |
| 04 | Learner Fit Engine | PLANNED |
| 05 | Distributed Content System | PLANNED |
| 06 | Review Workflow | PLANNED |
| 07 | Audio Engine | PLANNED |
| 08+ | Future Extensions | PLANNED |

---

# Phase 01 - Core Data Architecture

Status: IN PROGRESS

## Goals

Establish the foundational architecture for:

- Corpus ingestion
- Vocabulary storage
- Sentence generation
- Learner modelling
- Patch distribution
- Future audio support

## Database Architecture

### generation_cache.sqlite

Purpose:

- Raw corpus storage
- Source imports
- AI generation history
- Validation history
- Derived artifacts

Characteristics:

- Incrementally updateable
- Local-first
- Patchable

### common_content.sqlite

Purpose:

- Lexemes
- Rankings
- Sentence candidates
- Approved content
- Shared analysis

### learner_<id>.sqlite

Purpose:

- Learner knowledge estimates
- Review scheduling
- CI+1 calculations
- Progress tracking

## Common vs Learner Separation

Common database stores:

- Language facts
- Shared content
- Shared analysis

Learner database stores:

- Knowledge probabilities
- Sentence suitability
- Review history
- Personal progress

## Core Classes

### CorpusCache

Responsible for:

- Source tracking
- Revision tracking
- Derived artifacts
- Incremental updates

### CorpusIngestor

Responsible for:

- Corpus imports
- Frequency imports
- Sentence extraction

### LexemeRepository

Responsible for:

- Lexeme storage
- Ranking calculations
- Token resolution

### SentenceAnalyzer

Responsible for:

- Grammar analysis
- Naturalness scoring
- Ambiguity scoring
- Inference clue extraction

### LearnerModel

Responsible for:

- Knowledge estimates
- Exposure history
- Review scheduling

### LearnerSentenceFitScorer

Responsible for:

- Known-word percentage
- Learner inference score
- CI+1 score

### PatchManager

Responsible for:

- Patch creation
- Patch application
- Manifest handling
- Synchronization

## Versioning

Track:

- schema_version
- content_version
- validator_version
- rank_version
- patch_version

## Distributed Content Architecture

Technology:

- SQLite
- Google Drive
- rclone

Design:

- Immutable patches
- Stable machine IDs
- Central manifest
- Local patch ledger

---

# Phase 02 - Vocabulary Engine

Status: PLANNED

Summary:

- Import HSK
- Import CC-CEDICT
- Import frequency sources
- Lexeme ranking

---

# Phase 03 - Sentence Engine

Status: PLANNED

Summary:

- Candidate generation
- Candidate analysis
- Grammar detection
- Naturalness validation

---

# Phase 04 - Learner Fit Engine

Status: PLANNED

Summary:

- Probability-known model
- Learner inference scoring
- CI+1 calculations

---

# Phase 05 - Distributed Content System

Status: PLANNED

Summary:

- Patch distribution
- Synchronization
- Manifest management

---

# Phase 06 - Review Workflow

Status: PLANNED

Summary:

- Human review
- Content approval
- Audit workflow

---

# Phase 07 - Audio Engine

Status: PLANNED

Summary:

- TTS generation
- Slow playback
- Shadowing playback
- Audio validation

NOTE:
Audio has intentionally been deferred until the language-content pipeline is proven.

---

# Future Extensions

- Multi-language support
- Placement testing
- Collaborative review
- Shared corpus marketplace
- Learning analytics
- Hosted synchronization services

---

# Snapshot Notes

This document is a complete project snapshot.

Rules:

1. Every version contains all phases.
2. Completed phases remain fully documented.
3. The active phase receives the most detailed documentation.
4. Future phases remain summarized.
5. Future versions expand detail as phases are completed.
6. Final version will contain complete implementation notes for all phases.
