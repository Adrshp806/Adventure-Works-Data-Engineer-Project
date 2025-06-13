
# 🚀 Adventure Works Data Engineering Project (End-to-End on Azure)

This project demonstrates a complete data engineering pipeline using Azure cloud services, Databricks, and Power BI — inspired by the **Adventure Works dataset**. It follows the classic **Bronze-Silver-Gold architecture** for data lakes and applies best practices in data ingestion, transformation, and reporting.
![WhatsApp Image 2025-06-13 at 22 24 27_6868a0b0](https://github.com/user-attachments/assets/9e7738df-6889-4fa0-884f-cf94e1d1e4f1)

---

## 🗂️ Architecture Overview

```mermaid
graph TD
    A[HTTP Data Source] --> B[Azure Data Factory];
    B --> C[Data Lake Gen2 - Raw (Bronze)];
    C --> D[Databricks - Transformation];
    D --> E[Data Lake Gen2 - Transformed (Silver)];
    E --> F[Azure Synapse - Serving (Gold)];
    F --> G[Power BI - Reporting];
```

![Architecture Diagram](A08B614C-A411-425E-9308-7FAB7B98D4AD.png)

---

## 🔧 Tools & Services Used

| Layer        | Technology Used                       |
|--------------|----------------------------------------|
| Ingestion    | Azure Data Factory                    |
| Storage      | Azure Data Lake Storage Gen2 (Bronze, Silver) |
| Processing   | Azure Databricks (PySpark)            |
| Serving      | Azure Synapse Analytics (SQL Serverless) |
| Visualization| Power BI                              |

---

## 📌 Project Workflow

### 1. **Data Ingestion (Bronze Layer)**
- Data source: HTTP CSV file
- Used **Azure Data Factory** to ingest raw data into **Data Lake Gen2 (Bronze container)**

### 2. **Data Transformation (Silver Layer)**
- Performed transformations (type casting, null handling, deduplication, etc.) in **Azure Databricks using PySpark**
- Saved the cleaned data into the **Silver container** of the Data Lake

### 3. **Data Modeling & Serving (Gold Layer)**
- Created **External Tables & Views** in **Azure Synapse Serverless SQL Pool**
- Used `OPENROWSET` and `EXTERNAL TABLE` to serve data from the Silver layer

### 4. **Reporting**
- Connected **Power BI Desktop** to Synapse Serverless
- Built dashboards using the modeled AdventureWorks sales data

---

## 🧪 Key Features

- 🌊 Data Lakehouse architecture (Bronze, Silver, Gold)
- 📈 Real-time reporting using Power BI
- 🔄 Scalable ETL with Databricks & ADF
- 🔐 Secure data access using scoped credentials and Synapse views


## 👨‍💻 Author

**Apoorv Panwar**  
📍 London, UK  
📧 [apoorvpanwar2333@gmail.com](mailto:apoorvpanwar2333@gmail.com)  
🔗 [LinkedIn](https://www.linkedin.com/in/apoorv-panwar-b216411ab)

---

## 📌 Future Improvements

- Add automated scheduling via Data Factory triggers
- Implement CI/CD for notebooks and SQL scripts
- Enable row-level security in Power BI

---

## 📝 License

This project is for educational and demonstration purposes only.
