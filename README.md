# 🚀 Medallion-Agentic-Intelligence-Data-Platform

### Agentic Lakehouse Intelligence Platform with Governance & Executive Copilot

---

## 🧠 Overview

**Medallion-Agentic-Intelligence-Data-Platform** is an end-to-end Agentic Lakehouse platform built on Databricks that combines:

* Medallion architecture (Bronze → Silver → Gold)
* Autonomous governance agents (Guardian, Drift, Policy)
* KPI intelligence layer
* LLM-powered business insight generation
* Executive chatbot (Business Copilot)
* Agent control chatbot (Genie Control Plane)

It transforms raw CSV data into governed, validated, policy-compliant business insights — and makes them conversationally accessible to leadership.

---

## 🏗️ Architecture

```
                         ┌───────────────────────┐
                         │      raw_csv_data     │
                         │  (External Source)    │
                         └───────────┬───────────┘
                                     │
                                     ▼
                         ┌───────────────────────┐
                         │       BRONZE          │
                         │  Raw Ingested Delta   │
                         └───────────┬───────────┘
                                     │
                     ┌───────────────┼
                     ▼               ▼                
            ┌────────────────┐  ┌───────────────┐          ┌────────────────┐
            │ Guardian Agent │  │ Drift Agent   │          │ Genie Control  │
            │ (DQ checks)    │  │ (Anomaly)     │ ───────> │ (Decision Log) │
            └────────┬───────┘  └───────┬───────┘          └────────────────┘
                     │                  │
                     └──────────┬───────┘
                                ▼
                     ┌───────────────────────┐
                     │         SILVER        │
                     │ Cleaned / Validated   │
                     └───────────┬───────────┘
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │   SILVER_QUARANTINE   │
                     │ Failed / Anomalous    │
                     └───────────┬───────────┘
                                 │
                                 ▼
                         ┌────────────────┐
                         │  Policy Agent  │
                         │ (Business Rule │
                         │ Enforcement)   │
                         └────────┬───────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼                           ▼
         ┌───────────────────────┐   ┌────────────────────┐
         │          GOLD         │   │  GOLD_QUARANTINE   │
         │ Policy-Approved Data  │   │ Policy Violations  │
         └───────────┬───────────┘   └────────────────────┘
                     │
                     ▼
            ┌───────────────────────┐
            │       KPI TABLES      │
            │ revenue, churn, etc   │
            └───────────┬───────────┘
                        │
                        ▼
            ┌────────────────────────────┐
            │  Business Insight Agent    │
            │ (LLM Summary Generator)    │
            └───────────┬────────────────┘
                        │
                        ▼
            ┌────────────────────────────┐
            │   final_insights_table     │
            │ Structured AI Output       │
            └───────────┬────────────────┘
                        │
                        ▼
            ┌────────────────────────────┐
            │     Business Chatbot       │
            │ (Streamlit + SQL + LLM)    │
            └────────────────────────────┘

```

Parallel Layer:

```
Agent Decision Logs → Genie Chatbot (Control Plane)
```

---

## 🧱 Medallion Data Layers

### 🥉 Bronze

* Raw ingestion from CSV
* Delta format
* Immutable source layer

### 🥈 Silver

* Cleaned, validated data
* Schema enforcement
* Quality checks applied

### 🥇 Gold

* Policy-compliant curated data
* KPI-ready aggregations

---

## 🤖 Agentic Governance Framework

### 🛡 Guardian Agent

* Schema validation
* Null checks
* Threshold enforcement
* Writes to:

  * `silver`
  * `silver_quarantine`
  * `guardian_decision_log`

---

### 📉 Drift Agent

* Detects statistical drift
* KPI anomaly detection
* Flags abnormal variance
* Writes to:

  * `drift_alerts`
  * `anomaly_score_table`

---

### 📜 Policy Agent

* Business rule enforcement
* Compliance validation
* SLA verification
* Writes to:

  * `gold`
  * `gold_quarantine`
  * `policy_decision_log`

---

## 📊 KPI Intelligence Layer

From gold tables, KPI tables are generated:

* Revenue
* Growth %
* Churn
* Operational metrics
* Risk flags

These feed the AI insight generation layer.

---

## 🧠 Business Insight Agent (LLM)

Consumes structured KPI tables and generates:

* Executive summaries
* Revenue drivers
* Risk classification
* Actionable insights

Outputs stored in:

```
final_insights_table
```

This ensures:

* No repeated LLM calls
* Cost efficiency
* Structured AI outputs
* Full auditability

---

## 💬 Business Chatbot

Built using:

* Streamlit
* Databricks SQL Connector
* Databricks Model Serving (OpenAI-compatible endpoint)

Capabilities:

* Answers executive KPI questions
* Uses only `final_insights_table`
* No hallucination (context-constrained prompting)
* Deployed as Databricks App

---

## 🧠 Genie Chatbot (Agent Control Plane)

Genie monitors:

* Guardian decisions
* Drift alerts
* Policy violations
* Quarantine tables

Example queries:

* “Why were 23 records quarantined yesterday?”
* “What drift alerts triggered this week?”
* “Which policy rule failed most often?”

This separates:

* Business Intelligence
* Governance Intelligence

---

## 🔐 Governance & Observability

* Decision logs stored in Delta tables
* Quarantine isolation for failed records
* Clear audit trail per agent
* Structured JSON LLM outputs
* Separation of data vs insight vs governance

---

## ⚙️ Tech Stack

* Databricks Lakehouse
* Delta Tables
* Medallion Architecture
* Python
* Databricks SQL Connector
* Databricks Model Serving
* OpenAI-compatible API
* Streamlit (Databricks Apps)
* MLflow (optional tracking)

---

## 🚀 Deployment

### 1️⃣ Configure Environment Variables

In Databricks App settings:

```
DATABRICKS_HOST
DATABRICKS_SQL_HTTP_PATH
DATABRICKS_TOKEN
LLM_ENDPOINT_NAME
```

---

### 2️⃣ Install Requirements

```
streamlit
openai
databricks-sql-connector
```

---

### 3️⃣ Deploy App

```bash
databricks apps deploy business-insights-chatbot
```

---

## 📈 Why This Project Matters

Medallion-Agentic-Intelligence-Data-Platform demonstrates:

* Agentic AI applied to enterprise data engineering
* Governance-first LLM integration
* Controlled AI outputs stored as structured assets
* Conversational access to governed intelligence
* Cost-aware LLM design (no per-query full table scans)
* Production-ready Databricks App deployment

This is not a demo chatbot.
It is an **AI-augmented Lakehouse system**.

---

## 🧠 Key Design Principles

* Separation of concerns (data vs governance vs intelligence)
* No raw data exposure to LLM
* Quarantine-first validation
* Structured AI output storage
* Decision traceability
* Scalable architecture

---

## 📌 Future Enhancements

* Embedding-based semantic retrieval
* Vector search for KPI context
* Role-based access control
* Real-time streaming ingestion
* Auto-remediation agents
* Multi-agent orchestration framework

---

## 🏁 Final Statement

Medallion-Agentic-Intelligence-Data-Platform is a blueprint for the next generation of:

> Agentic, Governed, Conversational Data Platforms.

It combines data engineering, governance automation, and generative AI into a unified enterprise intelligence system.
