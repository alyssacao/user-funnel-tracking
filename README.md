# E-commerce User Funnel Tracking

## 📌 Overview

This project focuses on tracking and analyzing the user journey through an e-commerce website using a data pipeline built with Kafka, AWS S3, and AWS Glue. The goal is to understand how users move through the conversion funnel—from landing on the site to completing a purchase—and to identify key drop-off points or behavior trends using SQL-based analysis.

## ⚙️ Pipeline Workflow

1. **Kafka Producer (`KafkaProducer.ipynb`)**
   - Reads event data from `user_funnel_metrics.csv`
   - Publishes records (user actions like `view_product`, `add_to_cart`, `purchase`) into a Kafka topic.

2. **Kafka Consumer (`KafkaConsumer.ipynb`)**
   - Consumes streaming data from Kafka.
   - Writes raw event data to an AWS S3 bucket in real time.
   - AWS Glue Crawler scans the S3 location to create a unified table in the AWS Glue Data Catalog.

3. **Data Lake + Glue Table**
   - All user events are combined into structured tables for querying.
   - This makes the data SQL-queryable via AWS Athena or other BI tools.

4. **Queries (`Queries.sql`)**
   - **Transform Queries**: Generate cleaned funnel-stage tables by processing session-level event data. For example, create a Parquet-format table with compressed, timestamp-cast stage information, user device categories standardized, and filtering out incomplete or invalid stage timing data.
   - **Queries**: Provide insights such as:
     - **Hourly User Activity Count**
     - **User Count by Funnel Stage**
     - **Top Product Categories**
     - **Average Time to Next Funnel Stage**
     - **Stage Completion Rate** (e.g., % of users who completed checkout)
    
## Next step
Use Apache Airflow to automate and orchestrate the entire workflow, ensuring all data processing steps run reliably and on schedule.
