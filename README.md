# 📊 Wikimedia Streaming Data Pipeline (Bronze → Silver → Gold with SCD Type 2)

## 🚀 Overview

This project demonstrates a **production-style streaming data pipeline** built using:

* **Python Producer** (Wikimedia live stream)
* **File-based streaming ingestion**
* **Delta Lake (Bronze → Silver → Gold)**
* **Databricks Delta Live Tables (DLT)**
* **SCD Type 2 implementation for dimensional modeling**

The pipeline simulates a real-world **event-driven architecture** while remaining compatible with **Databricks free/serverless environments**.

---

## 🏗️ Architecture

```
Wikimedia Stream API
        ↓
Python Producer
        ↓
JSON Files (Streaming Simulation)
        ↓
Databricks (Auto Loader / Streaming Read)
        ↓
Bronze Layer (Raw Data)
        ↓
Silver Layer (Structured Data)
        ↓
Gold Layer (Aggregations + SCD Type 2)
```

---

## 📁 Project Structure

```
wikimedia-eventhub-databricks-pipeline/
│
├── transformations/
│   ├── wikimedia_bronze.py
│   ├── wikimedia_silver.py
│   ├── wikimedia_gold_scd2.py
│
├── producer/
│   ├── wikimedia_producer.py
│   ├── config.py
│
├── explorations/
├── utilities/
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 🔄 Data Flow

### 🟤 Bronze Layer

* Ingests raw JSON data from streaming files
* Stores data without transformation
* Schema:

  * `body` (JSON string)
  * `timestamp`
  * `offset`
  * `partition`
  * `topic`

---

### ⚪ Silver Layer

* Parses JSON into structured columns
* Cleans and prepares data for analytics
* Key fields:

  * `user`
  * `title`
  * `timestamp`
  * `bot`
  * `namespace`

---

### 🟡 Gold Layer

#### 1. Aggregations

* Edits per user
* Edits per minute
* Top edited pages
* Bot vs human activity

#### 2. SCD Type 2 (Dimension Table)

Tracks historical changes for users:

| Column       | Description      |
| ------------ | ---------------- |
| user         | Business key     |
| latest_title | Last edited page |
| is_bot       | Bot flag         |
| __START_AT   | Record start     |
| __END_AT     | Record end       |
| __IS_CURRENT | Active record    |

---

## 🧠 Key Concepts Demonstrated

* Structured Streaming
* Delta Lake architecture
* Delta Live Tables (DLT)
* Slowly Changing Dimensions (Type 2)
* Event-driven ingestion
* Schema evolution & parsing
* Layered data modeling

