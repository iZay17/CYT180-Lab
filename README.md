RFM Customer Segmentation Analysis
Course: CYT180 — Data Analytics | Seneca Polytechnic | 2026
Tools: Python, PySpark, PySpark MLlib, PySpark SQL

Overview
This project builds a large-scale customer segmentation pipeline using Python and Apache Spark (PySpark) to analyze a retail transaction dataset containing 541,909 rows across 8 columns and 4,373 unique customers spanning 20+ countries.
The goal is to segment customers using the RFM model — Recency, Frequency, and Monetary value — a proven framework used in marketing analytics and business intelligence to identify high-value customers, at-risk customers, and growth opportunities.

Dataset

Source: Online Retail dataset (CSV)
Size: 541,909 transactions
Fields: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country
Date Range: December 2010 — December 2011
Geographic Coverage: 20+ countries including United Kingdom, Germany, France, Spain, Belgium, and others


Technical Approach
1. Data Ingestion and Exploration

Loaded CSV into a PySpark DataFrame using spark.read.csv with schema inference and multiLine support
Performed initial data quality checks using .count(), .printSchema(), and .show() to validate structure and identify nulls
Identified 541,909 total records and 4,373 unique CustomerIDs

2. Data Cleaning and Timestamp Parsing

Parsed the InvoiceDate string column into a proper timestamp using F.coalesce() with multiple date format fallbacks to handle inconsistent raw data formats
Used F.min() and F.max() to identify dataset time boundaries for recency anchor calculation

3. RFM Feature Engineering

Recency: Computed time difference in seconds between each customer's most recent transaction and the dataset anchor date using PySpark Window functions (Window.partitionBy().orderBy()) to isolate the latest transaction per customer
Frequency: Counted distinct invoice occurrences per CustomerID using groupBy and countDistinct
Monetary Value: Calculated TotalAmount = Quantity × UnitPrice per transaction, then aggregated cumulative spend per customer using groupBy().agg(sum())
Joined Recency, Frequency, and Monetary DataFrames into a single clean RFM table using inner joins

4. Geographic Distribution Analysis

Segmented customers by country using groupBy('Country').agg(countDistinct('CustomerID')) ordered descending
United Kingdom represented the largest customer base with 3,950 unique customers

5. Feature Preparation for Machine Learning

Used VectorAssembler from pyspark.ml.feature to combine the three RFM columns into a single features vector column — required format for PySpark ML algorithms
Applied StandardScaler to normalize the features vector, ensuring no single RFM metric dominates the model due to scale differences (e.g. monetary value in hundreds vs frequency in single digits)
Output: standardized feature vectors ready for clustering or further modelling


Key Results
MetricValueTotal transactions processed541,909Unique customers analyzed4,373Countries represented20+Largest customer marketUnited Kingdom (3,950 customers)Dataset date rangeDec 2010 — Dec 2011

Technologies Used
ToolPurposePythonCore programming languageApache Spark / PySparkDistributed data processingPySpark SQLDataFrame operations and aggregationsPySpark MLlibVectorAssembler, StandardScalerPySpark Window FunctionsPer-customer recency calculationPandasData inspection and display

Skills Demonstrated

Large-scale data pipeline development
Feature engineering for machine learning
Distributed computing with Apache Spark
Data cleaning and timestamp normalization
Customer analytics and RFM modelling
Geographic data segmentation


Repository Structure
cyt180-rfm-analysis/
│
├── README.md
├── rfm_analysis.ipynb       # Main Jupyter notebook with full pipeline
└── data/
    └── Online_Retail.csv    # Source dataset (not included — too large)

Author
Isaiah Campbell
Cybersecurity and Threat Management | Seneca Polytechnic
LinkedIn | GitHub
