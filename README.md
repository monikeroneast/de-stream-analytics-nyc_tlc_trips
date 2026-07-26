# NYC Yellow Taxi Trip Analytics Pipeline

This repository contains an end-to-end data engineering pipeline that ingests, processes, and visualizes real-time and historical NYC Yellow Taxi trip data. 

This project was built as a capstone project for the **[DataTalksClub Data Engineering Zoomcamp](https://github.com)**. It is heavily inspired by and builds upon the structural foundations found in the various peer reference repositories as well.

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
* **[NotionLink]:** (https://bevel-chestnut-da0.notion.site/NYC-Yellow-Taxi-Analytics-Pipeline-3a893f2c74c080abb441f7d14d68c22f?pvs=25)

## About Me
Data Engineer and Analytics Engineer with 8+ years of experience spanning enterprise data architecture, cloud data platforms, and business intelligence, with a background in software applications, digital backend platforms, and connected vehicle ecosystems. Expert at defining data contracts and translating complex business requirements into production-ready data pipelines. Combines hands-on proficiency in Snowflake, BigQuery, Kafka, Spark and dbt with strong stakeholder and cross-functional delivery experience to deliver scalable, cost-optimized data assets.

Experienced across enterprise operations, systems, and cross-functional execution (Ex-Tata Motors), specializing in translating complex business problems into scalable, data-driven systems by working closely with engineering and analytics teams.

Currently based in India and open to Data Engineering and Analytics Engineering roles where I can bring both technical depth and stakeholder fluency from my product background. Happy to connect!

**[Notion Link]:** (https://bevel-chestnut-da0.notion.site/Jai-Lalita-Soren-Data-Engineering-Portfolio-32c93f2c74c080a6b4bae9fc24c8b730)
