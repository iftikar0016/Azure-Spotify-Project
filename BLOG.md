# 🎵 Spotify End-to-End Azure Data Engineering Project

  

![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)

![Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)

![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

  

> **A modern, scalable Data & AI Platform on Microsoft Azure.**

> This solution simulates a real-world music streaming service, handling the complete lifecycle from ingestion and processing to predictive modeling and conversational analytics.

  

---

  

## 📖 Table of Contents

  

- [Overview](#-overview)

- [Architecture](#-architecture)

- [Tech Stack](#-tech-stack)

- [Environment Setup](#-environment-setup--resource-creation)

- [Data Model](#-data-model-source)

- [Implementation Modules](#-implementation-modules)

  - [Data Ingestion (Source to Bronze)](#%EF%B8%8F-data-ingestion-source-to-bronze)

  - [Data Transformation (Bronze to Silver)](#-data-transformation-bronze-to-silver)

  - [Gold Layer (SCD Type 2 & Quality)](#-gold-layer-scd-type-2--data-quality)

  - [CI/CD & Production Deployment](#-cicd--production-deployment-databricks-asset-bundles)

  - [Machine Learning (Predicting Hits)](#-machine-learning-predicting-hits)

  - [AI Data Agent](#-ai-data-agent-chat-with-your-data)

- [Project Conclusion](#-project-conclusion)

  

---

  

## 🔍 Overview

  

This project builds a modern, scalable **Data & AI Platform** on Microsoft Azure. It goes beyond traditional data engineering by integrating **Machine Learning** and **Generative AI** directly into the pipeline.

  

The solution simulates a real-world scenario for a music streaming service (like Spotify), handling the complete lifecycle of data: from ingestion and processing to predictive modeling and conversational analytics.

  

**Key Features:**

- **🚀 Metadata-Driven Ingestion:** A dynamic, robust pipeline handling incremental loading for multiple tables using **Azure Data Factory**.

- **🏗️ Lakehouse Architecture:** Processing data through Bronze, Silver, and Gold layers using **Databricks** and **Delta Lake**.

- **🕰️ Advanced Data Modeling:** Implementing **SCD Type 2** to track historical changes using **Delta Live Tables (DLT)**.

- **🔮 Predictive Analytics (ML):** A machine learning model to predict potential "Hit Songs" based on audio features and metadata.

- **🤖 Generative AI Agent:** A "Chat with Data" interface powered by **LLMs (Llama 3)** that allows users to query the database using natural language.

- **🔄 CI/CD & DevOps:** Fully automated deployment using **Databricks Asset Bundles (DABs)**.

  

---

  

## 🏗️ Architecture

  

The solution follows the **Lakehouse / Medallion Architecture**:

  

1.  **Source:** **Azure SQL Database** acts as the on-premise/transactional source system containing raw music data.

2.  **Ingestion:** **Azure Data Factory (ADF)** orchestrates the movement of data from SQL to **Azure Data Lake Gen2** (Bronze Layer).

3.  **Transformation:** **Azure Databricks** processes the data:

    *   **Bronze:** Raw data landing zone.

    *   **Silver:** Cleaned and standardized data.

    *   **Gold:** Aggregated business-level data (Star Schema).

4.  **Serving:** Data is served via Databricks SQL / Azure Synapse for analysis.

5.  **Monitoring:** **Azure Logic Apps** sends email alerts on pipeline failures.

  

![Architecture Diagram](https://github.com/iftikar0016/Azure-Spotify-Project/blob/main/images/Architecture.png)

  

---

  

## 💻 Tech Stack

  

| Category | Technology |

|----------|------------|

| **Cloud Platform** | Microsoft Azure |

| **Ingestion** | Azure Data Factory (ADF) |

| **Storage** | Azure Data Lake Storage Gen2 (ADLS) |

| **Data Processing** | Azure Databricks (PySpark, Autoloader, Delta Live Tables) |

| **Database** | Azure SQL Database |

| **Version Control** | Git & GitHub |

| **Orchestration** | ADF & Azure Logic Apps |

| **AI/ML** | Scikit-Learn, MLflow, LangChain, Llama 3 |

  

---

  

## 🛠️ Environment Setup & Resource Creation

  

Before building the pipelines, the following resources were set up in a dedicated Resource Group (`RG_Azure_Project`):

  

### A. Azure Storage Account (Data Lake)

- **Resource:** Azure Data Lake Storage Gen2

- **Containers Created:**

  - `bronze`: For raw data ingestion.

  - `silver`: For cleaned/transformed data.

  - `gold`: For final aggregated data.

- **Configuration:** Enabled "Hierarchical Namespaces" to support folder structures.

  

### B. Azure Data Factory (ADF)

- **Resource:** Data Factory (`factory-azure-project`)

- **Git Integration:** Connected ADF to this GitHub repository.

  - **Repository Type:** GitHub

  - **Collaboration Branch:** `main`

  - **Feature Branch:** Created a new branch (e.g., `dev` or `anchal`) for development to follow CI/CD best practices.

  

### C. Azure SQL Database (The Source)

- **Resource:** Azure SQL Database & Server

- **Setup:**

  - Created a "Serverless" SQL database to optimize costs.

  - Configured Firewall rules to "Allow Azure Services" to access the DB.

- **Data Population:** Executed a SQL script to create and populate the initial tables:

  - `dbo.dim_users`

  - `dbo.dim_artists`

  - `dbo.dim_albums`

  - `dbo.dim_tracks`

  - `dbo.fact_streams`

  

![Azure Portal Resources](https://github.com/iftikar0016/Azure-Spotify-Project/blob/main/images/RG.png)

  

---

  

## 💾 Data Model (Source)

  

The source data mimics a Spotify database schema:

*   **`dim_users`**: User details (ID, Name, Country, Subscription Type).

*   **`dim_artists`**: Artist details (ID, Name, Genre).

*   **`dim_tracks`**: Track information (ID, Name, Duration, Release Date).

*   **`fact_streams`**: Transactional table recording every stream event.

  

---

  

## 🚀 Implementation Modules

  

### 🛠️ Data Ingestion (Source to Bronze)

  

#### 1. The Strategy: Metadata-Driven & Incremental

Instead of reloading the entire database every day (Full Load), this project uses an **Incremental Loading** strategy. We only fetch data that has changed or been added since the last run.

To achieve this, I implemented a **Watermarking** technique using external control files rather than a control table in the database.

  

#### 2. Linked Services (Connections)

Two reusable Linked Services were created to establish connections:

*   **`ls_AzureSqlDatabase`**: Connects to the source Azure SQL Database using SQL Authentication (stored securely).

*   **`ls_AdlsGen2`**: Connects to the destination Azure Data Lake Storage Gen2.

  

#### 3. Pipeline Logic: `pl_ingest_incremental`

This pipeline handles the extraction logic for a single table. It is designed to be **generic** and accepts parameters (`SchemaName`, `TableName`, `WatermarkColumn`) so it can be reused for any table.

  

**The Workflow:**

  

**Step A: Fetch Last Load Timestamp (Watermark)**

*   **Activity:** Lookup

*   **Source:** A JSON file (`cdc.json`) stored in the Data Lake.

*   **Purpose:** Retrieves the timestamp of the last successful data load (e.g., `2023-01-01 10:00:00`).

  

**Step B: Extract Data (Source to Bronze)**

*   **Activity:** Copy Data

*   **Source:** Azure SQL Database

*   **Logic:** Used a **Dynamic SQL Query** to fetch only new records.

    ```sql

    SELECT * FROM @{pipeline().parameters.SchemaName}.@{pipeline().parameters.TableName}

    WHERE @{pipeline().parameters.WatermarkColumn} > '@{activity('LookupWatermark').output.firstRow.value}'

    ```

*   **Sink:** Azure Data Lake Gen2 (Bronze Container).

*   **Format:** Parquet (Snappy Compressed) for efficient storage.

  

**Step C: Calculate New Watermark**

*   **Activity:** Script / Lookup

*   **Logic:** Queries the source table immediately after extraction to find the new maximum timestamp.

    ```sql

    SELECT MAX(@{pipeline().parameters.WatermarkColumn}) as NewWatermark

    FROM @{pipeline().parameters.SchemaName}.@{pipeline().parameters.TableName}

    ```

  

**Step D: Update Watermark File**

*   **Activity:** Copy Data

*   **Logic:** Writes the `NewWatermark` value back to the `cdc.json` file in the Data Lake, ready for the next run.

  

![Incremental Pipeline](https://github.com/iftikar0016/Azure-Spotify-Project/blob/main/images/ADF_IncrementalLoop.png)

  

#### 4. Handling "No New Data" (Optimization)

A common issue in incremental pipelines is generating empty files when no new data exists in the source.

*   **Solution:** Implemented an **If Condition** block after the Copy Activity.

*   **Check:** `@{activity('CopyData').output.rowsRead} > 0`

    *   **True:** Proceed to update the Watermark file.

    *   **False:** Do not update the watermark. Instead, use a **Delete Activity** to remove the empty file generated in the Bronze layer to keep the lake clean.

  

#### 5. Orchestration: The "Master" Loop

To avoid creating five separate pipelines for five tables, I created a parent pipeline: **`pl_master_ingestion`**.

*   **Activity:** ForEach Loop

*   **Input:** An array of metadata objects defining the tables to be loaded.

    ```json

    [

      {"Schema": "dbo", "Table": "dim_users", "Col": "updated_at"},

      {"Schema": "dbo", "Table": "fact_streams", "Col": "stream_date"}

    ]

    ```

*   **Execution:** The loop iterates through this list and triggers the `pl_ingest_incremental` pipeline for each item, passing the dynamic parameters.

  

---

  

### 🧠 Data Transformation (Bronze to Silver)

  

#### 1. Unity Catalog & Environment Setup

To ensure governance and security, I avoided the old "Mount Point" approach and used **Unity Catalog** with **External Locations**.

*   **Metastore & Catalog:** Created a dedicated catalog named `spotify_catalog`.

*   **External Locations:** Configured external locations to securely access ADLS Gen2 containers (`bronze`, `silver`, `gold`) without hardcoding credentials in notebooks.

*   **Compute:** Used **Serverless Compute** for fast startup times and efficient scaling.

  

#### 2. Ingestion Strategy: Auto Loader (`cloudFiles`)

Instead of standard batch processing, I used **Databricks Auto Loader** (`cloudFiles`). This allows the system to process data continuously and efficiently as files land in the lake.

*   **Schema Evolution:** Enabled `cloudFiles.schemaEvolutionMode` to "rescue" data. If the source schema changes (e.g., a new column is added), the pipeline doesn't fail; instead, it captures the extra data in a `_rescued_data` column.

*   **Checkpointing:** Maintained state using checkpoints to ensure **Exactly-Once** processing (idempotency).

  

**Code Snippet (Reading Stream):**

```python

df = spark.readStream.format("cloudFiles") \

    .option("cloudFiles.format", "parquet") \

    .option("cloudFiles.schemaLocation", schema_location) \

    .load(bronze_path)

```

![Auto Loader Read](https://github.com/iftikar0016/Azure-Spotify-Project/blob/main/images/Autoloader_read.png)

  

#### 3. Data Cleaning & Modular Coding (Utilities)

To keep the code clean and reusable, I applied **Software Engineering Best Practices**:

*   **Custom Utilities:** instead of rewriting logic repeatedly, I created a Python class `Reusable` inside a `utils` folder.

*   **Functionality:** Created methods like `drop_columns` to handle repetitive tasks like removing the `_rescued_data` column after processing.

*   **Path Management:** Used `sys.path.append` to dynamically import these custom modules into the main notebook.

  

**Transformations Applied:**

*   **`dim_users`:** Standardized usernames to Uppercase using `upper()`.

*   **`dim_tracks`:** Created a derived column `duration_flag` (Short/Medium/Long) using conditional logic (`when/otherwise`).

*   **`dim_artists`:** Removed special characters from names using **Regex** (`regexp_replace`).

  

#### 4. Advanced: Metadata-Driven Transformation (Jinja Templates)

For the complex joins in the Silver layer (creating Business Views), I implemented a **Metadata-Driven** approach using **Jinja Templates**.

*   **The Problem:** Writing static SQL queries for every join scenario is tedious and hard to maintain.

*   **The Solution:** I created a generic SQL template using Jinja (a Python templating engine).

    1.  Defined a Dictionary containing table names, join conditions, and required columns.

    2.  Passed this dictionary to the Jinja renderer.

    3.  The system **Dynamically Generates the SQL Query** at runtime.

  

This demonstrates the ability to build flexible frameworks rather than just writing hardcoded SQL.

  

**Jinja Code Snippet:**

```python

from jinja2 import Template

  

query_text = """

            SELECT

                {% for param in parameters %}

                    {{ param.cols }}

                        {% if not loop.last %} , {% endif %}

                {% endfor %}

            FROM

                {% for param in parameters %}

                    {% if loop.first %} {{ param['table'] }} AS {{ param['alias'] }} {% endif %}

                {% endfor %}

                {% for param in parameters %}

                    {% if not loop.first %}

                    LEFT JOIN {{ param['table'] }} AS {{ param['alias'] }} ON {{ param['condition'] }}

                    {% endif %}

                {% endfor %}

"""

# Renders the final SQL query dynamically

```

![Jinja Template Logic](https://github.com/iftikar0016/Azure-Spotify-Project/blob/main/images/jinja_sql_output.png)

  

---

  

### 🏆 Gold Layer (SCD Type 2 & Data Quality)

  

#### 1. The Goal: Slowly Changing Dimensions (SCD Type 2)

For the final "Gold" layer, we needed to track the **history of changes**.

*   **Scenario:** If a user changes their subscription plan or an artist changes their genre, we don't just want to overwrite the old data (SCD Type 1). We want to keep a record of *when* that change happened.

*   **Solution:** Implement **SCD Type 2**, which adds `start_date` and `end_date` columns to every record to track validity periods.

  

#### 2. The Tool: Delta Live Tables (DLT) & Declarative Pipelines

Instead of writing complex merge logic manually (which is error-prone), I used **Delta Live Tables (DLT)**. This allows for a **Declarative** approach—I simply define *what* I want (e.g., "Keep history based on these keys"), and DLT handles the *how* (inserts, updates, expiring old records).

  

![DLT Pipeline Graph](https://github.com/iftikar0016/Azure-Spotify-Project/blob/main/images/Pipeline_P2.png)

  

#### 3. Implementation Details (Python & DLT)

  

**Step A: Staging Layer (Reading Silver)**

First, I created a staging view that reads from the Silver table. This acts as the source for our SCD logic.

```python

import dlt

from pyspark.sql.functions import *

  

@dlt.table

def dim_user_staging():

    return spark.read.table("spotify_catalog.silver.dim_user")

```

  

**Step B: Applying SCD Type 2 Logic (Auto CDC)**

I used the `dlt.apply_changes()` function (also known as Auto CDC) to automatically handle the history tracking.

*   **`target`**: The final Gold table (`dim_user`).

*   **`keys`**: The primary key (`user_id`).

*   **`sequence_by`**: The column used to order events (`updated_at`). This ensures that even if data arrives out of order, the pipeline picks the latest value correctly.

*   **`stored_as_scd_type`**: Set to `"2"` to enable history tracking.

  

```python

dlt.create_streaming_table("dim_user")

  

dlt.apply_changes(

    target = "dim_user",

    source = "dim_user_staging",

    keys = ["user_id"],

    sequence_by = col("updated_at"),

    stored_as_scd_type = "2"

)

```

  

#### 4. Data Quality Expectations (Validation)

To ensure reliable data in the Gold layer, I implemented **Expectations** (Data Quality Rules).

*   **Rule:** `expect_all_or_drop`

*   **Logic:** If a record fails the validation (e.g., `user_id` is Null), it is automatically dropped from the pipeline to prevent corrupting the downstream reports.

  

```python

@dlt.table

@dlt.expect_all_or_drop({"valid_id": "user_id IS NOT NULL"})

def dim_user_clean():

    # ... logic ...

```

  

#### 5. Managing the Pipeline (Lakeflow)

The entire workflow is managed in the **Lakeflow Pipelines** UI (formerly DLT UI).

*   **Mode:** Triggered (Batch) or Continuous (Streaming).

*   **Compute:** Serverless (for fast startup and scaling).

*   **Observability:** The UI provides a visual graph showing data lineage, processing speed, and the number of records dropped by validation rules.

  

![DLT Execution](https://github.com/iftikar0016/Azure-Spotify-Project/blob/main/images/gold_pl_completed.png)

  

---

  

### 🚀 CI/CD & Production Deployment (Databricks Asset Bundles)

  

#### 1. The Strategy: "Infrastructure as Code" (IaC)

In the previous steps, we manually created notebooks and pipelines. In a real-world production environment, this is not scalable. To follow DevOps best practices, I implemented **Databricks Asset Bundles (DABs)** to manage the entire Databricks project as code.

*   **Why DABs?** It allows us to package our Notebooks, Delta Live Tables, and Job definitions into a single bundle that can be version-controlled and deployed to multiple environments (Dev, QA, Prod) using a command line interface (CLI).

  

#### 2. Setting up the Bundle

I initialized the project using the Databricks CLI within the workspace terminal.

*   **Command:** `databricks bundle init`

*   **Structure:** This created a standardized project structure containing:

    *   `databricks.yml`: The master configuration file.

    *   `src/`: Folder containing my source code (Notebooks, DLT pipelines).

    *   `resources/`: Definitions for jobs and workflows.

  

#### 3. Configuration (`databricks.yml`)

The `databricks.yml` file is the brain of the deployment. I configured it to support **Multi-Environment Deployment**.

*   **Targets:** I defined two specific targets:

    1.  **`dev`**: Deploys to my personal development workspace. It uses a "development" mode where resources are isolated.

    2.  **`prod`**: Deploys to the production environment (simulated). It uses stricter permissions and a fixed "root path" to ensure stability.

  

```yaml

bundle:

  name: spotify_dab

  

targets:

  dev:

    mode: development

    default: true

    workspace:

      host: <my-dev-workspace-url>

  

  prod:

    mode: production

    workspace:

      host: <my-prod-workspace-url>

    root_path: /Shared/prod/spotify_project

```

  

#### 4. Deployment (The CI/CD Workflow)

Instead of manually clicking "Publish," I used the terminal to deploy the entire project.

  

**Step A: Validation**

First, I verified that my configuration file was syntactically correct: `databricks bundle validate`

  

**Step B: Deploy to Dev**

I deployed the code to my personal sandbox to test the changes: `databricks bundle deploy -t dev`

  

**Step C: Deploy to Prod**

Once validated, I deployed the exact same bundle to the production target. This ensures consistency between environments: `databricks bundle deploy -t prod`

  

![Deployment Terminal](https://github.com/iftikar0016/Azure-Spotify-Project/blob/main/images/databricks_terminal.png)

  

---

  

### 🤖 Machine Learning (Predicting Hits)

  

#### 1. The Business Problem

Beyond just storing data, I wanted to deliver predictive value. The goal was to build a classification model to identify **Potential Hits**—songs likely to achieve high streaming numbers—based on their metadata (genre, duration, artist, etc.).

  

#### 2. Data Preparation (Feature Engineering)

I leveraged the **Gold Layer** tables (`fact_streams`, `dim_track`, `dim_artist`) to create a training dataset.

*   **Target Variable:** Defined a "Hit" as a song with streams greater than the median stream count (Binary Classification: 1 = Hit, 0 = Non-Hit).

*   **Feature Selection:**

    *   **Numeric:** `duration_sec`, `release_year` (extracted from `release_date`).

    *   **Categorical:** `genre`, `artist_country`.

*   **Exclusion:** I explicitly excluded `dim_user` data because the model predicts *song* popularity, independent of specific user behaviors.

  

```python

import pyspark.sql.functions as F

  

# 1. Aggregate FactStream to get Target Variable (Total Streams per Track)

df_popularity = spark.sql("""

    SELECT track_id, COUNT(stream_id) as total_streams

    FROM spotify_cata.gold.factstream

    GROUP BY track_id

""")

  

# 2. Join with Dimensions to get Features

df_features = spark.sql("""

    SELECT

        t.track_id, t.duration_sec, t.release_date,

        a.genre, a.country as artist_country

    FROM spotify_cata.gold.dimtrack t

    JOIN spotify_cata.gold.dimartist a ON t.artist_id = a.artist_id

""")

  

# 3. Final Dataset & Feature Engineering

df_full = df_features.join(df_popularity, "track_id", "left").fillna(0)

df_full = df_full.withColumn("release_year", F.year("release_date"))

  

# Convert to Pandas for Scikit-Learn (Limit to 100k rows if data is huge)

pdf = df_full.limit(100000).toPandas()

```

  

#### 3. Model Training & MLflow Integration

I used **Scikit-Learn** within Databricks to build a Random Forest Classifier.

*   **Pipeline:** Implemented a Scikit-Learn `Pipeline` with a `ColumnTransformer` to handle preprocessing automatically:

    *   **StandardScaler** for numeric features.

    *   **OneHotEncoder** (with `handle_unknown='ignore'`) for categorical features.

*   **MLflow Tracking:** I used `mlflow.sklearn.autolog()` to automatically log experiment runs, parameters (e.g., `n_estimators=100`, `max_depth=10`), and metrics.

  

**Results:**

The initial model achieved an accuracy of ~65%, providing a baseline for identifying potential hits based purely on metadata.

  

![MLflow Metrics](https://github.com/iftikar0016/Azure-Spotify-Project/blob/main/images/ML_score.png)

  

---

  

### 💬 AI Data Agent (Chat with Your Data)

  

#### 1. The Concept: Natural Language to SQL

To democratize data access for non-technical stakeholders (like music executives), I built an **AI Agent**. This allows users to ask questions in plain English (e.g., *"Who are the top 3 artists?"*) and receive accurate data answers immediately, without writing SQL.

  

#### 2. Tech Stack

*   **Orchestration:** **LangChain** (specifically `create_sql_query_chain`).

*   **LLM:** **Groq (Llama-3.3-70b)**. I chose Groq for its ultra-low latency, which is critical for real-time interactive agents.

*   **Execution Engine:** **SparkSQL**. The agent generates Spark-compliant SQL queries that run directly against the Delta Tables.

  

#### 3. Implementation Details

*   **Virtual Database:** I configured the agent to see only specific Gold tables (`fact_streams`, `dim_artists`, `dim_tracks`) to ensure it focuses on relevant business data.

*   **Prompt Engineering:** I designed a strict `PromptTemplate` that instructs the LLM to:

    1.  Use specific table definitions.

    2.  Return **ONLY** the syntactically correct SparkSQL query.

    3.  Avoid any markdown or conversational filler.

  

```python

from langchain_classic.chains import create_sql_query_chain

from langchain_core.prompts import PromptTemplate

  

# Custom prompt that instructs LLM to return only SQL

custom_prompt = PromptTemplate.from_template(

    """Given an input question, create a syntactically correct {dialect} query to run.

    Only use the following tables:

    {table_info}

    Question: {input}

    Return ONLY the SQL query without any explanation.

    SQL Query:"""

)

  

# 1. Chain that generates the SQL query with custom prompt

write_query = create_sql_query_chain(llm, spark_sql, prompt=custom_prompt)

  

# 2. Function that executes the SQL query using Spark

def execute_query(query):

    result = spark.sql(query.strip())

    return result.toPandas().to_string()

  

# 3. Combine them: Write Query -> Execute Query -> Answer

chain = write_query | execute_query

```

  

#### 4. Workflow

User Question $\rightarrow$ LLM (Generates SQL) $\rightarrow$ Spark (Executes Query) $\rightarrow$ Final Answer.

  

**Example:**

*   **Input:** "What is the average duration of songs in the Pop genre?"

*   **Generated SQL:** `SELECT average_duration FROM ... JOIN ...`

*   **Output:** `212.7 seconds`

  

![AI Agent Output](https://github.com/iftikar0016/Azure-Spotify-Project/blob/main/images/Ai_Agent_Output.png)

  

---

  

## 🎓 Project Conclusion

  

**Summary:**

This project successfully demonstrates a full-stack data capability, transforming raw raw transactional data into intelligence.

1.  **Ingested** & **Processed** complex datasets at scale using Azure Data Factory and Databricks.

2.  **Ensured Quality** and historical accuracy using Medallion Architecture and SCD Type 2 strategies.

3.  **Unlocked Value** by deploying a Random Forest Classifier to identify future trends (Hit Prediction).

4.  **Democratized Access** by building an AI Agent that removes the technical barrier to entry, allowing stakeholders to "just ask" questions in plain English.

  

**Business Value Delivered:**

*   ✅ **Operational Efficiency:** Automated data flows reduce manual reporting time by 100%.

*   ✅ **Strategic Insight:** The ML model helps A&R teams identify promising tracks early, potentially increasing revenue.

*   ✅ **Self-Service Analytics:** The AI Agent empowers non-technical executives to get instant answers without waiting for data analysts to write SQL.

  

---

*Created by Iftikar | [GitHub Profile](https://github.com/iftikar0016)*
