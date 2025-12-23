# Open Air Nature ETL/ELT Pipeline

## 🏗️ Project Architecture & Workflow
This project demonstrates a production-grade End-to-End Data Engineering Pipeline that automates the flow of air quality data from an external API to a live interactive dashboard.

## 🛠️ Tech Stack
- **Orchestration:** Apache Airflow (Dockerized)

- **Data Source:** OpenWeather Air Pollution API

- **Data Lake:** Google Cloud Storage (GCS)

- **Data Warehouse:** Google BigQuery

- **Transformation:** dbt (data build tool)

- **Visualization:** Streamlit (Interactive Dashboard)

- **Environment:** Docker & Docker Compose

## 🔄 Data Flow (Step-by-Step)
**1. Extraction (Airflow):** A Python-based DAG triggers daily (or manually) to fetch real-time air quality data (PM2.5, PM10, AQI) for the Taunus Mountains region via OpenWeather API.

**2. Staging (GCS):** The raw JSON data is instantly uploaded to a Google Cloud Storage bucket, serving as our immutable Data Lake.

**3. Loading (BigQuery):** Using the GCSToBigQueryOperator, the raw data is ingested into BigQuery landing tables.

**4. Transformation (dbt):**

- ***Staging Layer:*** Raw data is cleaned and cast to correct types (e.g., FLOAT64 for PM2.5).

- ***Analytics Layer:*** Final tables are created for downstream consumption, ensuring high performance and data integrity.

**5. Visualization (Streamlit):** An interactive dashboard connects to BigQuery to visualize:

- Latest air quality metrics.

- Historical trends of PM2.5.

- ***Interactive Alerts:*** A dynamic threshold slider that allows users to set custom warning limits.

## Streamlit Dashboard - Dark Mode

<img width="1321" height="645" alt="Screenshot 2025-12-23 at 13 42 51" src="https://github.com/user-attachments/assets/62044f42-ac89-40a2-bf60-51ad8a12d7ba" />


<img width="1306" height="620" alt="Screenshot 2025-12-23 at 13 43 07" src="https://github.com/user-attachments/assets/de79bdb7-2db3-4cae-854a-6231f8a23578" />

## Airflow Pipeline (DAG View)

<img width="1241" height="108" alt="Screenshot 2025-12-23 at 14 24 00" src="https://github.com/user-attachments/assets/902be095-7d22-4d3b-8b7e-f889c10c3b01" />

## BigQuery Data Preview

<img width="864" height="191" alt="Screenshot 2025-12-23 at 14 25 22" src="https://github.com/user-attachments/assets/4a8ec56a-2c28-46ba-a560-bd4eaad9fcbd" />

