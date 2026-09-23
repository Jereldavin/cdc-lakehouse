# cdc-lakehouse
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-316192?logo=postgresql&logoColor=white)](#)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-2.5+-231F20?logo=apachekafka&logoColor=white)](#)
[![Debezium](https://img.shields.io/badge/Debezium-CDC-990000?logo=debezium&logoColor=white)](#)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72C48?logo=minio&logoColor=white)](#)
[![Databricks](https://img.shields.io/badge/Databricks-DLT_%26_Unity_Catalog-FF3621?logo=databricks&logoColor=white)](#)

My capstone project where i **replicate a modern enterprise Change Data Capture (CDC) (Notion, Grab, etc) Lakehouse architecture locally**—without expensive cloud-managed streaming infrastructure—while seamlessly integrating with **Databricks Unity Catalog**, **Delta Live Tables (DLT)**, and **AI/BI Dashboards** (Databricks Trial)

## 🏗 Architecture Overview
```mermaid
flowchart TD
    subgraph LOCAL ["Local Infrastructure (Docker Compose)"]
        direction TB
        PG[("PostgreSQL 14<br>(WAL Logical Replication)")]
        DBZ["Debezium Connector<br>(Kafka Connect :8083)"]
        KAFKA{{"Apache Kafka<br>(:9092)"}}
        BRIDGE["Python Bridge<br>(bridge_minio.py)"]
        MINIO[("MinIO S3 Storage<br>(Bucket: cdc-raw)")]
        
        PG -->|Write-Ahead Log pgoutput| DBZ
        DBZ -->|Topics: dbserver1.inventory.*| KAFKA
        KAFKA -->|Consume Batches of 50| BRIDGE
        BRIDGE -->|Put JSONL Files| MINIO
    end
    
    subgraph TRANSFER ["Manual Landing Bridge"]
        direction TB
        DL["1. Download .jsonl files<br>from MinIO Console (:9001)"]
        UL["2. Upload .jsonl files<br>to Databricks Volume UI"]
        
        DL --> UL
    end
    
    subgraph CLOUD ["Databricks Lakehouse (DLT)"]
        direction TB
        UC_VOL[("Databricks Unity Catalog<br>Volume: /Volumes/.../cdc_raw/")]
        BRONZE[("🥉 Bronze Layer<br>read_files() Auto Loader")]
        SILVER[("🥈 Silver Layer<br>SCD Type 2 (APPLY CHANGES INTO)")]
        GOLD[("🥇 Gold Layer<br>gold_customer_ltv KPI Aggregation")]
        DASH["📊 Databricks AI/BI Dashboard<br>(Lakeview Visualization)"]
        
        UC_VOL --> BRONZE
        BRONZE --> SILVER
        SILVER --> GOLD
        GOLD --> DASH
    end

    %% Cross-subgraph connections must be defined outside the subgraphs
    MINIO --> DL
    UL --> UC_VOL
