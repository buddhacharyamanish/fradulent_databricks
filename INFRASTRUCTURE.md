# Project Infrastructure & Technology Stack

A simple, layman-friendly guide to the computers, software, and cloud services running this pipeline.

---

## 1. Quick Summary (In Plain English)

Think of this project like a postal service that never sleeps:
1. **Source**: An **AWS MySQL database** records user actions (like logins and transactions).
2. **Postman**: **Debezium & Apache Kafka** pick up each change immediately and hold the messages in a queue.
3. **Delivery Truck**: A single **Azure virtual machine** running **Apache Spark** continuously reads those messages every 10 seconds.
4. **Destination**: The data is neatly unpacked and saved into a **Delta Lake table** inside **Databricks Unity Catalog**, stored securely in Azure Cloud Storage.

---

## 2. List of Technologies Used

### Cloud Providers
* **Microsoft Azure**: Hosts the Databricks workspace, virtual machine compute, and cloud data storage (located in Sydney / Australia East).
* **Amazon Web Services (AWS)**: Hosts the source MySQL database and the Kafka message broker.

### Compute & Virtual Machines (The "Computers")
* **Single-Node Setup**: Instead of paying for multiple separate computers, everything runs on **1 single virtual machine** (called a Single-Node cluster). This is fast, simple, and saves a lot of money.
* **Machine Size (`Standard_D2ds_v6`)**:
  * **Processor (Cores)**: 2 CPU cores (minimal sufficient footprint to process streaming transactions without exceeding Azure quotas).
  * **Memory (RAM)**: 8 GB of memory (used to hold and process incoming micro-batches).
  * **Temporary Disk Space**: High-speed local SSD disk (used for scratch files and temporary buffers).
* **Worker Nodes**: **0 workers** (the single machine handles both management and data processing).

### Data Processing & Big Data Engine
* **Databricks Runtime (DBR 15.4 LTS)**: The operating environment provided by Databricks, tested for long-term stability.
* **Apache Spark 3.5**: The engine that reads, filters, and transforms data in real time.
* **PySpark (Python 3.11)**: Python code used to write the pipeline logic.
* **Scala 2.12**: The underlying language Spark is built with.

### Storage & Database Management
* **Azure Data Lake Storage Gen2 (ADLS Gen2)**: Microsoft's cloud storage where all data files are physically stored (like a giant, secure hard drive in the cloud).
* **Delta Lake 3.2**: A smart storage layer built on top of cloud files. It acts like a regular database table (supports updates, prevents corrupted data, and keeps history).
* **Unity Catalog**: The governance system in Databricks that organizes data into a 3-level hierarchy:
  * **Catalog**: `fradulenttransactionmonitoring`
  * **Schema**: `bronze`
  * **Table**: `activity_log_staging`
* **Unity Catalog Volume**: A special folder (`fraud_checkpoints`) used to remember which messages were already read, so nothing is ever processed twice or missed if the machine restarts.

### Message Streaming & Source Database
* **MySQL 8.0**: The source relational database where transactions originate.
* **Debezium (CDC)**: A tool that watches the MySQL database's log files and turns every new row or edit into a message.
* **Apache Kafka**: The real-time messaging queue (running at `34.228.32.86:9092`) that buffers the messages until Databricks reads them.

### Deployment & Tooling
* **Databricks Asset Bundles (DAB)**: The tool that packages our code and automatically sets up the job and cluster on Azure Databricks with one command.
* **Serverless SQL Warehouse**: A small, on-demand query engine in Databricks used to inspect the tables and run validation queries without needing to turn on the main cluster.

---

## 3. How the Computer Runs the Job

* **Continuous Running**: The job is set to stay on 24/7. If Azure restarts the machine for maintenance, Databricks automatically turns it back on.
* **Batch Trigger (Every 10 Seconds)**: Every 10 seconds, the machine asks Kafka: *"Are there any new messages?"* If yes, it pulls them, saves them, and waits for the next 10-second mark.
* **Safety Speed Limit**: The pipeline limits itself to processing a maximum of 10,000 records per batch so the machine never runs out of memory.
