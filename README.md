# F1 analytics using Azure and Databricks 

## Introduction  
This project is a hands-on implementation of a **data lakehouse architecture** on Microsoft Azure, using **Databricks, Delta Lake, and Azure Data Factory (ADF)**. The goal was to explore the end-to-end data engineering lifecycle—data ingestion, transformation, governance, and making the curated data available for downstream analytics via Power BI. The dataset used is from the **Formula1 Ergast dataset**, which provides historical F1 race results from 1950.  

---

## Tech Stack  
- Python  
- PySpark  
- SQL  
- Azure Cloud Services (key components)  
  - Azure Databricks  
  - Azure Data Lake Storage Gen2 (ADLS)  
  - Azure Data Factory (ADF)  
  - Azure Key Vault  
  - Power BI  


---

## Motivation  
While preparing for the **Databricks Data Engineer certification**, I wanted to build a real-world project that went beyond theory. Formula1 data offered a fun and challenging dataset to apply concepts such as medallion architecture, incremental loads, and Delta Lake.  

---

## Overview  

### 1. Ingestion  
- Data files sourced from the Ergast dataset.  
- Multiple formats ingested: **CSV, JSON, multi-line JSON, and multiple file batches**.  
- Data stored in **ADLS Raw container**.  
- Audit columns (ingestion date, source) added for traceability.  

### 2. Preprocessing  
- Standardized column names (lowercase, underscores).  
- Dropped unnecessary fields (URLs).  
- Partitioned large tables (e.g., races by year) for performance.  

### 3. Transformation  
- Data moved from **Raw → Processed → Presentation layers** using **Delta Lake**.  
- Built incremental pipelines in **PySpark** to avoid duplicates and reduce cost.  
- Created derived tables using **SQL + PySpark** for:  
  - Driver standings  
  - Constructor standings  
  - Combined race insights  

### 4. Final Data for Analysis  
- Curated tables in the **Presentation layer**.  
- Connected to **Power BI** for reporting and business consumption.  

---

## Key Learnings  
- Difference between **full load vs incremental load**, and why incremental is critical in production.  
- How **Delta Lake** solves challenges of traditional data lakes (ACID transactions, schema evolution, rollback, and time travel).  
- Effective use of **PySpark and SQL** for transformations in Databricks.  
- Role of **Unity Catalog** in governance and secure access management.  
- Importance of pipeline orchestration (scheduling, failure alerts, monitoring) in production-ready solutions.  
- Trade-offs in **horizontal vs vertical scaling** in Spark clusters.  

---

## Translating to Production Environments  
While this project was built as a learning exercise, the architecture is production-ready with a few extensions:  
- Replace manual ingestion with **ADF pipelines** connected to live APIs or event streams.  
- Automate schema validation and add **data quality checks**.  
- Implement **CI/CD pipelines** with DevOps for deploying notebooks and workflows across environments.  
- Use **Unity Catalog** for enterprise-grade governance and secure data access.  
- Expose curated data to **multiple BI/AI consumers** (Power BI, ML models, APIs).  

---

## Conclusion  
This project helped me gain practical, hands-on experience with Databricks, PySpark, SQL, and the Azure ecosystem, while applying real-world data engineering practices to Formula1 data. It bridges theory with practice and demonstrates how cloud-native platforms can deliver scalable, governed, and analytics-ready data.  
