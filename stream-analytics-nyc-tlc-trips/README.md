# NYC Yellow Taxi Trip Analytics Pipeline

This repository contains an end-to-end data engineering pipeline that ingests, processes, and visualizes real-time and historical NYC Yellow Taxi trip data. 

This project was built as a capstone project for the **[DataTalksClub Data Engineering Zoomcamp](https://github.com)**. It is heavily inspired by and builds upon the structural foundations found in the various peer reference repositories as well. **[de-zoomcamp-project1](https://github.com)**.

## 🏗 Architecture & Workflow

The architecture uses a mix of streaming infrastructure, cloud storage, data warehousing optimizations, and downstream transformation workflows:

```text
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌───────────────┐
│  NYC Taxi   │────▶│    Kafka     │────▶│     GCS      │────▶│   BigQuery    │
│ Parquet Data│     │  Streaming   │     │ (Data Lake)  │     │  (Warehouse)  │
└─────────────┘     └──────────────┘     └──────────────┘     └───────┬───────┘
                                                                      │
                                                                      ▼
┌─────────────┐                                               ┌───────────────┐
│  Streamlit  │◀──────────────────────────────────────────────│   dbt Core    │
│ (Dashboard) │                                               │(Transformations)
└─────────────┘                                               └───────────────┘
```

1. **Infrastructure as Code:** Google Cloud Platform (GCP) resources are provisioned deterministically via **Terraform**.
2. **Ingestion & Streaming:** A Python-based **Apache Kafka** producer simulates real-time ingestion, streaming batch taxi events to a dedicated Kafka topic. A Kafka consumer ingests the stream directly into a **Google Cloud Storage (GCS)** data lake.
3. **Data Warehousing:** Raw cloud storage objects are batch-loaded into **Google BigQuery**, utilizing strict **partitioning** (by pickup date) and **clustering** (by location/payment type) to keep analytical query costs low.
4. **Data Transformation:** Analytics-ready models are built inside BigQuery using **dbt Core**, modularized into staging layers and semantic analytical marts.
5. **Visualization:** Downstream transformed business metrics are surfaced through an interactive, browser-accessible **Streamlit** dashboard showing trip distributions, monthly trends, and pricing analysis.

## 🛠 Tech Stack

* **Cloud Platform:** Google Cloud Platform (GCP)
* **IaC:** Terraform
* **Message Broker:** Apache Kafka (Confluent / Docker Containerized)
* **Data Lake:** Google Cloud Storage (GCS)
* **Data Warehouse:** Google BigQuery
* **Transformation Layer:** dbt Core
* **Dashboard / Frontend:** Streamlit
* **Containerization:** Docker & Docker Compose

