# 🔍 Supply Chain Fraud Detection using Graph Analytics on Snowflake

> **Real-time fraud intelligence platform** that uses Neo4j Graph Analytics on Snowflake to detect fraudulent seller networks, shell entities, and high-risk actors across e-commerce supply chains (Amazon & Flipkart).

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://supply-chain-fraud-detection-haumvgl3uvmti2m52xa4cb.streamlit.app)

---

## 🎯 Problem Statement

E-commerce platforms like Amazon and Flipkart face a growing threat from **organized seller fraud rings** — groups of fake sellers that share bank accounts, inflate return rates, and exploit logistics networks. Traditional rule-based fraud detection systems fail because:

- **Fraudsters operate as networks**, not individuals — they share financial infrastructure, collude on pricing, and rotate identities across platforms
- **Static threshold rules** miss evolving fraud patterns and generate excessive false positives
- **No entity resolution** — the same real-world entity registers as multiple sellers with different GST numbers but the same bank account

### The Cost of Inaction
- Global e-commerce fraud losses exceeded **$48 billion in 2023** (Juniper Research)
- Return fraud alone costs retailers **$101 billion annually** (NRF)
- Marketplace platforms lose **2-5% of GMV** to seller-side fraud

---

## 💡 Solution — Graph-Powered Fraud Intelligence

This project builds an **end-to-end fraud detection pipeline** that models the supply chain as a graph and applies advanced algorithms to uncover hidden fraud patterns that traditional methods miss.

### Why Graphs?

| Traditional Approach | Graph Approach |
|---|---|
| Analyze sellers in isolation | Analyze sellers **in context** of their network |
| Static rules (return rate > X%) | Dynamic scoring using **PageRank centrality** |
| No entity resolution | **WCC** detects shell companies sharing bank accounts |
| Manual investigation | **Louvain communities** auto-detect fraud clusters |

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA GENERATION (Python)                      │
│  1,000 Sellers · 60 Warehouses · 20,000 Orders · 10 Fraud Rings│
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     SNOWFLAKE (Data Platform)                    │
│  Tables: SELLERS, WAREHOUSES, ORDERS, LOGISTICS                 │
│  Graph Tables: SELLERS_GRAPH, WAREHOUSES_GRAPH, ORDERS_GRAPH    │
│  SHARED_BANK_EDGES_GRAPH (fraud ring connections)               │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              NEO4J GRAPH ANALYTICS (Native App)                  │
│                                                                  │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────────┐ │
│  │   PageRank   │ │   Louvain    │ │  Weakly Connected        │ │
│  │  Centrality  │ │  Community   │ │  Components (WCC)        │ │
│  │              │ │  Detection   │ │  Entity Resolution       │ │
│  └──────┬───────┘ └──────┬───────┘ └────────────┬─────────────┘ │
│         │                │                       │               │
│  Seller influence   Fraud clusters     Shell company detection   │
│  in supply chain    & collusion rings  via shared bank accounts  │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   RISK SCORING ENGINE                             │
│                                                                  │
│  Risk Score = (Return Rate × 30) + (PageRank × 20)              │
│             + (Fraud Flag × 50)                                  │
│                                                                  │
│  HIGH > 60  |  MEDIUM > 30  |  LOW ≤ 30                         │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              REAL-TIME PIPELINE (Snowflake Native)               │
│                                                                  │
│  Streams ──► Dynamic Table (1-min lag) ──► Alerts ──► Email     │
│  (CDC)       SELLER_RISK_REALTIME          HIGH_RISK   Notifier │
│                                                                  │
│  Scheduled Tasks: Re-run graph algorithms every 15 minutes       │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    STREAMLIT DASHBOARD                            │
│                                                                  │
│  Internal (Snowsight) ──── streamlit_app.py                      │
│  Public   (Cloud)     ──── streamlit_app_public.py               │
│                                                                  │
│  5 Tabs: Network Graph | Fraud Rings | Analytics | PageRank     │
│          | Risk Table                                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🧠 Graph Algorithms Used

### 1. PageRank — Seller Influence Scoring
- **What it does**: Measures how "central" a seller is in the supply chain network based on order flow to warehouses
- **Why it matters**: High-PageRank fraudulent sellers have disproportionate influence — they route orders through many warehouses to obscure fraud patterns
- **Configuration**: Damping factor = 0.85, Max iterations = 20

### 2. Louvain Community Detection — Fraud Cluster Identification
- **What it does**: Groups sellers and warehouses into communities based on dense order connections
- **Why it matters**: Fraud rings tend to cluster — they ship through the same warehouses, creating unusually dense subgraphs that Louvain identifies automatically
- **Output**: Each seller gets a `LOUVAIN_COMMUNITY` ID revealing which cluster they belong to

### 3. Weakly Connected Components (WCC) — Entity Resolution
- **What it does**: Finds groups of sellers connected through shared bank accounts
- **Why it matters**: The same real-world entity often registers multiple seller accounts with different names but the same bank account. WCC collapses these into a single `WCC_ENTITY` — revealing that 5 "different" sellers are actually one fraudster
- **Result**: 184 suspicious bank-sharing pairs detected across 1,000 sellers

---

## 📊 Key Metrics & Findings

| Metric | Value |
|---|---|
| Total Sellers Analyzed | 1,000 |
| Warehouses in Network | 60 |
| Orders Processed | 20,000 |
| Fraud Rings Injected | 10 (50 sellers) |
| Shared Bank Account Pairs | 184 |
| High-Risk Sellers Detected | 42 |
| Graph Algorithm Runtime | < 30 seconds |
| Dashboard Refresh Lag | 1 minute (Dynamic Table) |

---

## 🏗️ Tech Stack

| Component | Technology |
|---|---|
| **Data Platform** | Snowflake |
| **Graph Analytics** | Neo4j Graph Analytics (Snowflake Native App) |
| **Real-Time Pipeline** | Snowflake Streams + Dynamic Tables + Tasks + Alerts |
| **Data Generation** | Python (Faker, NumPy, Pandas) |
| **Visualization** | Streamlit, Plotly, NetworkX |
| **Email Alerts** | Snowflake Notification Integration |
| **Deployment** | Snowsight (internal) + Streamlit Community Cloud (public) |

---

## 📁 Project Structure

```
Supply-Chain-Fraud-Detection/
├── .devcontainer/
│   └── devcontainer.json          # GitHub Codespaces config
├── notebooks/
│   ├── supply_chain_data_generator.ipynb   # Synthetic data generation
│   └── supply_chain_graph_viz.ipynb        # Network visualization notebook
├── sql/
│   └── supplyChain.sql            # Complete SQL pipeline (schema + graph + real-time)
├── streamlit/
│   ├── streamlit_app.py           # Snowsight-hosted dashboard (uses get_active_session)
│   └── streamlit_app_public.py    # Public dashboard (uses snowflake.connector)
├── requirements.txt               # Python dependencies
├── LICENSE
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- Snowflake account with `ACCOUNTADMIN` role
- [Neo4j Graph Analytics](https://app.snowflake.com/marketplace/listing/Neo4j%20Graph%20Analytics) Native App installed from Snowflake Marketplace
- Python 3.11+ (for data generation notebook)

### Step 1: Set Up Snowflake Infrastructure
```sql
-- Run the complete SQL pipeline
-- File: sql/supplyChain.sql

USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS SUPPLY_CHAIN_DB;
CREATE WAREHOUSE IF NOT EXISTS SUPPLY_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

### Step 2: Generate Synthetic Data
Open `notebooks/supply_chain_data_generator.ipynb` in Snowflake Notebooks or Jupyter:
- Generates 1,000 sellers, 60 warehouses, 20,000 orders
- Injects 10 fraud rings (5 sellers each sharing a bank account)
- Uploads all data to Snowflake tables
- Creates graph-ready tables for Neo4j

### Step 3: Run Graph Analytics
```sql
-- PageRank — Seller influence scoring
CALL NEO4J_GRAPH_ANALYTICS.graph.page_rank('CPU_X64_XS', {
    'project': {
        'nodeTables': ['SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                       'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH'],
        'relationshipTables': {
            'SUPPLY_CHAIN_DB.PUBLIC.ORDERS_GRAPH': {
                'sourceTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                'targetTable': 'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH'
            }
        }
    },
    'compute': { 'dampingFactor': 0.85, 'maxIterations': 20 },
    'write': [{'nodeLabel': 'SELLERS_GRAPH',
               'outputTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLER_PAGERANK'}]
});

-- Louvain — Community detection
CALL NEO4J_GRAPH_ANALYTICS.graph.louvain('CPU_X64_XS', { ... });

-- WCC — Entity resolution via shared bank accounts
CALL NEO4J_GRAPH_ANALYTICS.graph.wcc('CPU_X64_XS', { ... });
```

### Step 4: Build Risk Scoring Table
```sql
CREATE OR REPLACE TABLE SELLER_RISK_MASTER AS
SELECT
    s.nodeId, s.seller_name, s.platform, s.city,
    s.return_rate, s.fraud_flag, s.bank_account,
    p.pagerank AS pagerank_score,
    c.community AS louvain_community,
    w.component AS wcc_entity,
    ROUND((s.return_rate * 30) + (p.pagerank * 20) + (s.fraud_flag * 50), 2) AS risk_score,
    CASE
        WHEN (s.return_rate*30 + p.pagerank*20 + s.fraud_flag*50) > 60 THEN 'HIGH'
        WHEN (s.return_rate*30 + p.pagerank*20 + s.fraud_flag*50) > 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_level
FROM SELLERS s
LEFT JOIN SELLER_PAGERANK p ON s.nodeId = p.nodeId
LEFT JOIN SELLER_COMMUNITIES c ON s.nodeId = c.nodeId
LEFT JOIN SELLER_WCC w ON s.nodeId = w.nodeId;
```

### Step 5: Enable Real-Time Pipeline
```sql
-- Dynamic Table with 1-minute refresh
CREATE OR REPLACE DYNAMIC TABLE SELLER_RISK_REALTIME
  TARGET_LAG = '1 minute' WAREHOUSE = SUPPLY_WH AS ...;

-- Scheduled graph algorithm refresh (every 15 min)
CREATE OR REPLACE TASK GRAPH_ANALYTICS_ROOT
  WAREHOUSE = SUPPLY_WH SCHEDULE = '15 MINUTE' AS ...;

-- Alert on new HIGH-risk sellers (every 5 min)
CREATE OR REPLACE ALERT HIGH_RISK_SELLER_ALERT
  WAREHOUSE = SUPPLY_WH SCHEDULE = '5 MINUTE' ...;

-- Email notifications (every 10 min)
CREATE OR REPLACE TASK FRAUD_EMAIL_NOTIFIER
  WAREHOUSE = SUPPLY_WH SCHEDULE = '10 MINUTE' AS ...;
```

### Step 6: Deploy Dashboard

**Internal (Snowsight):**
1. Go to Snowsight → Streamlit → Create Streamlit App
2. Upload `streamlit/streamlit_app.py`
3. Select `SUPPLY_WH` warehouse and `SUPPLY_CHAIN_DB` database

**Public (Streamlit Community Cloud):**
1. Fork this repo
2. Go to [share.streamlit.io](https://share.streamlit.io) → New App
3. Set main file: `streamlit/streamlit_app_public.py`
4. Configure secrets in Advanced Settings:
```toml
[snowflake]
account = "your-account.region.cloud"
user = "YOUR_SERVICE_USER"
password = "your_password"
warehouse = "SUPPLY_WH"
database = "SUPPLY_CHAIN_DB"
schema = "PUBLIC"
role = "YOUR_READ_ONLY_ROLE"
```

---

## 📸 Dashboard Preview

### 🕸 Network Graph — Seller → Warehouse Connections
Interactive force-directed graph with 500+ nodes. Node size represents PageRank centrality, color represents risk level (🔴 High, 🟡 Medium, 🟢 Low). Blue diamonds are warehouses.

### 🔴 Fraud Ring Detection — WCC Entity Resolution
Visualizes clusters of sellers sharing the same bank account. Each connected component represents a single real-world entity operating multiple fake accounts.

### 📊 Analytics — Multi-Dimensional Risk Analysis
- Risk distribution by platform (Amazon vs Flipkart)
- Geographic risk heatmap by city
- Community-level risk aggregation
- Overall risk composition

### 📈 PageRank — Centrality vs Risk Scatter
Identifies the most dangerous sellers: those with both high network influence (PageRank) and high fraud risk scores.

### 📋 Risk Table — Filterable Seller Database
Complete seller risk database with filters by platform and risk level.

---

## 🔬 How the Risk Score Works

```
Risk Score = (Return Rate × 30) + (PageRank Score × 20) + (Fraud Flag × 50)
```

| Factor | Weight | Rationale |
|---|---|---|
| **Return Rate** | 30% | High return rates indicate potential return fraud or product misrepresentation |
| **PageRank Score** | 20% | High centrality in order network suggests the seller is routing orders through many warehouses (a common fraud obfuscation tactic) |
| **Fraud Flag** | 50% | Known fraud indicators from historical data and shared bank account detection |

| Risk Level | Score Range | Action |
|---|---|---|
| 🔴 HIGH | > 60 | Immediate investigation — likely part of fraud ring |
| 🟡 MEDIUM | 30–60 | Enhanced monitoring — suspicious patterns detected |
| 🟢 LOW | ≤ 30 | Normal monitoring — no significant risk indicators |

---

## 🔄 Real-Time Pipeline

The system operates continuously without manual intervention:

| Component | Type | Frequency | Purpose |
|---|---|---|---|
| `ORDERS_STREAM` | Stream | Continuous | Captures new order data via CDC |
| `SELLERS_STREAM` | Stream | Continuous | Captures new seller registrations |
| `SELLER_RISK_REALTIME` | Dynamic Table | 1 minute | Auto-refreshes risk scores as data changes |
| `GRAPH_ANALYTICS_ROOT` | Task DAG | 15 minutes | Re-runs PageRank, Louvain, WCC algorithms |
| `HIGH_RISK_SELLER_ALERT` | Alert | 5 minutes | Logs new HIGH-risk sellers to FRAUD_ALERT_LOG |
| `FRAUD_EMAIL_NOTIFIER` | Task | 10 minutes | Sends email summary of recent fraud alerts |

---

## 🛡️ Security

- **Role-Based Access Control**: Public dashboard uses a dedicated read-only role (`STREAMLIT_PUBLIC_READER`) with SELECT access to only 4 required tables
- **Service User**: Dedicated `STREAMLIT_APP_USER` with minimal privileges — no ACCOUNTADMIN exposure
- **Secrets Management**: Snowflake credentials stored in Streamlit secrets (never hardcoded)

---

## 🏆 Why This Project Stands Out

1. **Graph-native fraud detection** — not just rules, but network analysis that catches organized fraud rings traditional systems miss
2. **End-to-end on Snowflake** — data, compute, graph analytics, real-time pipeline, alerts, and dashboard all on one platform
3. **Production-ready architecture** — Streams, Dynamic Tables, Tasks, and Alerts create a fully automated pipeline
4. **Dual deployment** — internal Snowsight dashboard for analysts + public Streamlit Cloud app for stakeholders
5. **Entity resolution** — WCC algorithm resolves duplicate identities that share bank accounts, collapsing fake accounts into real entities
6. **Scalable** — graph algorithms run on Snowflake compute, tested on 1,000+ nodes and 20,000+ edges

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Paras Jain** — [@ParasJain03](https://github.com/ParasJain03)
**Richa Grover** — [@RichaACN](https://github.com/RichaACN)
**Arpit Raj** — [@Arpit599222](https://github.com/Arpit599222)

---

*Built with ❄️ Snowflake · 🔗 Neo4j Graph Analytics · 🎈 Streamlit*
