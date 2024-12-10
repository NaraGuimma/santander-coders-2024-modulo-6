# ETL Performance Comparison: Pandas, PySpark, Polars, and PyArrow

## Overview
This project demonstrates a comparative analysis of ETL (Extract, Transform, Load) performance using four libraries: **Pandas**, **PySpark**, **Polars**, and **PyArrow**. By testing these frameworks across datasets of different sizes, we evaluate their efficiency in terms of runtime and output file size. Additionally, various file formats were used for data loading and storage to explore their compatibility and performance.

### Key Objectives
1. **Dataset Selection:**
   - **Small Dataset:** Extracted a subset (<10,000 rows) from a larger dataset.
   - **Large Dataset:** A dataset with over 1,000,000 rows.
   - Datasets used:
     - [Transactions Fraud Dataset](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets).
     - Titanic dataset.
     - Reviews dataset(From Airbnb Rio de Janeiro).
     - Locations dataset(From Airbnb Rio de Janeiro).

2. **ETL Steps:**
   - **Extraction:** Reading data from CSV files.
   - **Transformation:** Data cleaning (dropping rows with null values) and column renaming.
   - **Loading:** Writing transformed data into multiple file formats.

3. **Frameworks Compared:**
   - **Pandas:** Popular for small to medium datasets.
   - **PySpark:** Distributed computing suitable for large datasets; executed both locally and on Databricks.
   - **Polars:** A fast, DataFrame-like library written in Rust for high-performance ETL.
   - **PyArrow:** Focused on columnar data processing and file format interoperability.

4. **File Formats Evaluated:**
   - **CSV:** Simple and human-readable.
   - **Parquet:** Optimized for analytics with efficient storage and query performance.
   - **Avro:** Compact and schema-based serialization.
   - **Delta Lake:** Adds ACID transactions and versioning to Parquet (PySpark-only).
   - **ORC:** Highly optimized for compression and analytics.

5. **Comparative Analysis:**
   - Runtime for each ETL step (extraction, transformation, loading).
   - File size for output formats.
   - Visualization of results using [Briefer](https://github.com/briefer/notebook).

---

## Framework Introduction

### Pandas
- A Python-based library for data manipulation and analysis.
- Strengths: User-friendly, comprehensive API.
- Weaknesses: Performance bottlenecks with large datasets.

### PySpark
- A Python interface for Apache Spark, enabling distributed data processing.
- Strengths: Scales to massive datasets with cluster computing.
- Weaknesses: Requires setup and resource management.

### Polars
- A DataFrame library written in Rust, designed for speed and parallelism.
- Strengths: Extremely fast; supports lazy execution.
- Weaknesses: Less mature ecosystem compared to Pandas or Spark.

### PyArrow
- Focused on columnar in-memory data format and interoperability with Arrow-compatible tools.
- Strengths: Seamless integration with Parquet and Avro.
- Weaknesses: Primarily suited for data serialization rather than full ETL pipelines.

---

## File Format Introduction

### CSV
- Plain text format.
- Strengths: Simple and widely supported.
- Weaknesses: No compression or schema.

### Parquet
- Columnar storage format optimized for analytics.
- Strengths: Efficient compression and faster queries.

### Avro
- Row-based format with embedded schema.
- Strengths: Good for streaming and interoperability.

### Delta Lake
- An extension of Parquet with ACID transactions.
- Strengths: Ensures data reliability and supports time travel.

### ORC
- Optimized Row Columnar format, designed for Hive.
- Strengths: High compression and optimized performance.

---

## Project Structure
```plaintext
.
├── data/                    # Dataset samples
├── notebooks/               # Jupyter Notebooks for ETL steps
│   ├── pandas_etl.ipynb     # ETL pipeline with Pandas
│   ├── pyspark_etl.ipynb    # ETL pipeline with PySpark
│   ├── polars_etl.ipynb     # ETL pipeline with Polars
│   ├── pyarrow_etl.ipynb    # ETL pipeline with PyArrow
├── results/                 # ETL timing and file size results
├── briefer_uploads/         # Visualization data for Briefer
└── README.md                # Project documentation
```

---

## Dataset Details
The project utilizes four datasets of varying sizes to ensure a comprehensive performance comparison across small and large data scales.

| ID  | Dataset Name        | Number of Rows |
|-----|---------------------|----------------|
| 1   | transactions_data   | 13,305,914     |
| 2   | titanic             | 890            |
| 3   | reviews             | 703,795        |
| 4   | locations           | 843            |

### Rationale
- **transactions_data**: A very large dataset, ideal for testing the scalability of frameworks like PySpark and Polars.
- **titanic**: A classic small dataset often used in data science tutorials, serving as a benchmark for lightweight processing.
- **reviews**: A medium-sized dataset for bridging the gap between small and very large datasets.
- **locations**: Another small dataset that adds variety and ensures diverse ETL scenarios.

Each dataset was processed using all four frameworks (Pandas, PySpark, Polars, and PyArrow) for the ETL tasks. The results were analyzed to compare runtime, scalability, and output file sizes.

---

## Results Summary

### Runtime Comparison
<img width="1236" alt="image" src="https://github.com/user-attachments/assets/2db0b664-7069-406e-974f-b0f9db8fa9da">



---

## Visualization
The results were visualized using **Briefer**, enabling easy comparison of runtimes and file sizes. Visualizations are hosted in the `briefer_uploads/` directory and can be directly opened within the Briefer tool.

---

## Execution Environment

1. **Local Execution:** All frameworks were run locally on a machine with 16 GB RAM and 8 CPU cores.
2. **Databricks:** PySpark was also executed on Databricks to leverage Delta Lake capabilities.

---

## Setup
### Prerequisites
- Python 3.8+
- Jupyter Notebook
- Databricks account (for PySpark with Delta Lake)

### Running Notebooks
Open the notebooks in the `notebooks/` folder and run them sequentially for each framework.

### Visualizing Results
1. Upload the CSV result files from `results/` to Briefer.
2. Generate visualizations for runtime and file size comparisons.

---

## Insights and Learnings
1. **Performance:**
   - **Small Datasets:** Polars outperformed other frameworks in runtime.
   - **Large Datasets:** PySpark excelled due to its distributed computing capabilities.
2. **File Formats:** Parquet and ORC consistently produced smaller file sizes.
3. **Compatibility:** Delta Lake’s ACID properties and time travel make it ideal for enterprise-grade ETL.

---

## Future Work
- Explore integration with cloud storage (e.g., AWS S3, Google Cloud Storage).
- Extend the analysis to include real-time data streaming frameworks.
- Test additional file formats like JSON and Feather.

---

## License
This project is licensed under the MIT License. See the LICENSE file for details.

