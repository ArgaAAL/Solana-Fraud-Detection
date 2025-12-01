# 🕵️‍♂️ Solana Forensics

![Visitor Count](https://visitor-badge.laobi.icu/badge?page_id=ArgaAAL.Solana-Forensics)

[![GitHub Stars](https://img.shields.io/github/stars/ArgaAAL/Solana-Forensics?style=social)](https://github.com/ArgaAAL/Solana-Forensics/stargazers)


### *Institutional-Grade Ransomware, Money Laundering & Illicit Activity Detection Engine for the Solana Blockchain*

> **A high-throughput forensic ETL + neural inference engine designed to detect ransomware patterns, layering behavior, and malicious actors on Solana using behavioral analysis and deep learning.**

[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-blue.svg)]()
[![ONNX Runtime](https://img.shields.io/badge/Inference-ONNX-purple.svg)]()
[![Helius RPC](https://img.shields.io/badge/RPC-Helius-orange.svg)]()

---

# ⚡ Executive Summary

Traditional blockchain analysis tools break under Solana’s throughput. This engine solves that by combining:

* **Context-Aware ETL** (DeFi protocol detection, economic reconstruction)
* **Feature Engineering Optimized for Fraud Detection**
* **ONNX-accelerated Deep Learning (MLP)**
* **Adaptive Oracle Aggregation for pricing normalization**

Instead of static blacklists, **Solana Forensics fingerprint behavior** — measuring liquidity bursts, DEX graph interactions, temporal anomalies, and cross-program complexity to predict malicious intent with a confidence score.

---

# 🧠 Threat Model

The system targets detection of:

* **Ransomware payout pipelines**
* **Darknet service cashouts**
* **Wash trading & multi-hop layering**
* **Fast-moving mule wallets**
* **Flash-based liquidity laundering**
* **Programmatic bot activity disguised as humans**

Not designed for:

* AML/KYC identity attribution
* Generic trading analytics
* Real-time MEV/arb analytics

---

# 🏗️ System Architecture

```mermaid
graph LR
    A[RPC Ingestion Layer] -->|Raw TXs| B[Feature Engineering Engine]
    B -->|Feature Vectors| C[ONNX Inference Engine]
    C --> D["Risk Score (0.0 - 1.0)"]

    subgraph L1 [Layer 1: ETL]
        A
        A1[Transaction Parser]
        A2[Context Classifier]
        A --> A1 --> A2
    end
    
    subgraph L2 [Layer 2: Feature Engineering]
        B
        B1[Temporal Density]
        B2[DeFi Graph Metrics]
        B3[Price Normalization]
        A2 --> B1 & B2 & B3 --> B
    end
    
    subgraph L3 [Layer 3: ML Decision Plane]
        C
        D
    end
````

---

# 🧩 Core Components

## 1️⃣ SolanaDataExtractor — *Context-Aware ETL*

Located in `src/solana_extractor.py`.

Extracts not only what happened but **why** it happened by reconstructing transaction economics.

### Capabilities

* **DeFi Protocol Fingerprinting**
  Auto-detects Raydium, Orca, Jupiter, Marinade, lending protocols (Solend), stablecoin swaps, staking events, etc.

* **Multi-Layer Oracle Aggregation**
  Jupiter V3 → CoinGecko → CryptoCompare
  Ensures accurate USD normalization for all token movements.

* **Bot & Burst Behavior Detection**

  * `burst_activity_score`
  * `round_number_ratio`
  * `slot_density`
  * `success_rate`

---

## 2️⃣ SolanaFeatureCalculator — *Feature Synthesis Engine*

Transforms ETL output into ML-optimized numerical vectors.

### Features include:

* **Temporal Analysis**

  * Inter-tx timing variance
  * Slot-density
  * Burst frequency

* **Graph Metrics**

  * Unique counterparties
  * Program interaction ratios
  * Money-flow entropy

* **Transaction Complexity**
  Weighted protocol interactions (e.g., DEX_SWAP=3.0, TRANSFER=1.0).

---

## 3️⃣ SolanaRansomwareModelTester — *Inference Plane*

Located in `src/inference_engine.py`.

### Highlights

* **Sub-ms inference** via ONNX Runtime
* **Floating-point consistency checks** vs raw sklearn model
* **Production-ready manual standard scaling**
* **Portable across Python/Rust/C++/WASM deployments**

---

# 📦 Installation

### Requirements

* Python **3.10+**
* Helius API key
* Optional: CryptoCompare / Moralis keys

### Install

```bash
git clone https://github.com/YourUsername/Solana-Forensics.git
cd Solana-Forensics

pip install -r requirements.txt

cp .env.example .env
# Add API keys inside .env
```

---

# 🚀 Usage

## 🔍 1. Forensic Scan (Single Address)

```bash
python src/solana_extractor.py
```

Outputs a rich JSON profile including:

* defi_ratio
* programmatic_ratio
* success_rate
* burst_activity_score
* normalized token flows

---

## 🧪 2. Model Inference Test

```bash
python src/inference_engine.py
```

Sample output:

```
🚀 RUNNING SOLANA RANSOMWARE MODEL TEST
======================================================================
🧠 Model Type: MLP (Deep Neural Network)
🎯 AUC Score: 0.9421

📊 Malicious Probability: 0.8723
🔍 Classification: 🚨 MALICIOUS
```

---

# 📊 Performance Benchmarks

| Metric          | Score             | Notes                                      |
| --------------- | ----------------- | ------------------------------------------ |
| **AUC-ROC**     | **0.94**          | Strong separation between benign/malicious |
| **Precision**   | High              | Reduced false positives                    |
| **Inference**   | < 15ms            | ONNX, CPU optimized                        |
| **Scalability** | 20k+ addresses/hr | ETL batching enabled                       |

---

# 📂 Dataset Description

The training dataset contains:

* Known ransomware cashout clusters
* Darknet-linked mule wallets
* High-frequency bot swarms
* Benign trading activity
* DEX arbitrage clusters
* Airdrop distribution behavior

All addresses are public-domain blockchain entities.

---

# 🔄 End-to-End Pipeline Overview

1. **Fetch raw TXs** via Helius/Moralis
2. **Context classification** (DEX, lending, staking, etc.)
3. **Reconstruct token economics**
4. **Normalize using oracle pricing**
5. **Feature vector creation**
6. **Model prediction**
7. **Confidence-scored risk output**

---

# 🔧 Configuration

Environment variables:

```
HELIUS_API_KEY=
CRYPTOCOMPARE_KEY=
MORALIS_KEY=
RPC_URL=
```

---

# 🗺️ Roadmap

### 🚧 Phase 1 — Current

* ONNX inference
* Manual scaler consistency
* ETL + feature extraction

### 🛠️ Phase 2 — Planned

* Transformer-based anomaly detection
* L2-normalized vector store for similarity search
* Web dashboard (FastAPI + React)
* Real-time monitoring engine (websocket RPC)
* Solana Firedancer integration

### 🔬 Phase 3 — Research

* On-chain WASM inference
* Graph Neural Networks (GNN)
* Multi-chain correlation (BTC → SOL → CEX)

---

# 🤝 Contributing

PRs welcome!
Areas needing contributors:

* New heuristic detectors
* Protocol signature detection
* Benchmark improvements
* Dashboard/UI
* Dataset labeling

---

# ❓ FAQ

### **Does this identify real people?**

No. Only behavioral patterns on public blockchains.

### **Is this a law enforcement tool?**

Not directly — but it can support compliance teams, auditors, and researchers.

### **Can it run on-chain?**

The ONNX model is light enough for WASM or Solana program-side inference with modifications.

---

# 📜 License

MIT License — open and free for research.

Part of the **Crypto-Threat-Intelligence** suite.

---
