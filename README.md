# Intrafact

**Intrafact** is a self-updating AI knowledge system designed to transform scattered organizational information into a continuously evolving, queryable intelligence layer.

Instead of treating documents and discussions as static inputs, Intrafact models knowledge as a **living system** — automatically ingesting, structuring, and reasoning over information as work happens.

---

## Why Intrafact?

Modern teams generate knowledge everywhere:
- documents and PDFs  
- code commits and pull requests  
- internal discussions and decisions  

Yet this knowledge remains fragmented and hard to reason over.

**Intrafact solves this by:**
- continuously ingesting knowledge from multiple sources
- normalizing it into a canonical internal format
- storing it as semantic memory
- enabling context-grounded reasoning using RAG and agentic workflows

This project is intentionally built as an **MVP-first, system-design-focused implementation**, inspired by how real enterprise AI platforms are architected.

---

## High-Level Architecture

The diagram below shows the core architecture of Intrafact, from knowledge ingestion to reasoning and user interaction.

> 📌 **Architecture Overview**

![Intrafact Architecture](docs/Full_architecture.png)

**Key layers:**
- **External Knowledge Sources** – files, repositories, and discussions  
- **Ingestion Layer** – automated connectors and event listeners  
- **Normalization Layer** – text cleaning, metadata enrichment, canonicalization  
- **Knowledge Processing** – chunking, embeddings, deduplication  
- **Knowledge Storage** – vector database + metadata store  
- **Reasoning Layer** – Retrieval-Augmented Generation (RAG) with agent control  
- **User Interface** – query, feedback, and observability  

Each layer is intentionally decoupled to reflect production-grade system design.

---

## Core Design Principles

- **Living Knowledge, Not Static Uploads**  
  Knowledge is continuously ingested as work happens.

- **System Design Over Model Training**  
  Focus on pipelines, reasoning, and data flow — not training models from scratch.

- **Explainability First**  
  Every answer is grounded in retrieved context with traceable sources.

- **MVP Discipline**  
  The system is scoped carefully to what is built now vs. what is planned.

---

## MVP Scope (Built Now)

The current MVP focuses on the essential end-to-end loop:

- Local file ingestion (PDF, Markdown)
- GitHub commit and pull request ingestion
- Text normalization and chunking
- Vector database storage
- Single-agent RAG-based reasoning
- Simple query interface

This ensures a working, inspectable system before adding complexity.

---

## Planned Evolution

Intrafact is designed to evolve incrementally:

- **Extended MVP**
  - Discord message ingestion
  - Enhanced metadata filtering
  - Multi-pass retrieval
  - Agent collaboration workflows
  - Feedback-driven retrieval improvement

- **Production-Ready (Future)**
  - Authentication and role-based access
  - Real-time streaming ingestion
  - Monitoring, logging, and alerts
  - Multi-tenant knowledge isolation
  - Scalability and reliability controls

These phases are documented explicitly to demonstrate scope control and engineering maturity.

---

## Project Structure

```text
intrafact/
├── docs/                 # Architecture and design diagrams
├── data/                 # Raw and processed knowledge
├── intrafact/
│   ├── ingestion/        # Source connectors
│   ├── normalization/    # Cleaning and canonicalization
│   ├── processing/       # Chunking and embeddings
│   ├── storage/          # Vector and metadata stores
│   ├── retrieval/        # Semantic retrieval logic
│   └── reasoning/        # RAG and agentic reasoning
├── app.py                # Entry point
└── README.md
