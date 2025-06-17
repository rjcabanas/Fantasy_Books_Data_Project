**Project Overview**

This project collects data related to fantasy books from the Open Library API, including metadata on books, authors, and user reviews/ratings. The data is processed and stored using a medallion architecture (Bronze, Silver, Gold layers) on Azure Data Lake Gen2, with orchestration and analytics pipelines implemented using industry-standard tools.

**Tech Stack**
- Orchestration: Apache Airflow, Databricks Workflows
- Data Processing: Python, PySpark, Pandas
- Storage: Azure Data Lake Gen2
- Authentication: Azure Entra ID with "Storage Blob Contributor" role
- Visualization: Power BI
- File Formats: JSON (raw), Parquet (processed)

**Architecture: Medallion Layers**
- Bronze Layer: Raw ingested data from API, stored as combined Parquet files
- Silver Layer: Cleaned, structured data ready for modeling
- Gold Layer: Dimensional and fact tables optimized for analytics

**Pipeline Stages**

1. Data Ingestion (Apache Airflow + Python)
- Connects to the Open Library API
- Python scripts includes error handling with API rate limits
- Fetches paginated data for:
  - Fantasy books
  - Authors
  - Book reviews and ratings
- Stores the JSON responses locally
- Converts combined JSON into a Parquet file
- Uploads the Parquet file to the Bronze layer in Azure Data Lake Gen2
- Ingestion orchestration is handled using Apache Airflow

![image](https://github.com/user-attachments/assets/e74e6a4e-3d5c-4624-b9b3-702a5188ae41)

Apache Airflow DAG run screenshot.

2. Data Transformation (Databricks + PySpark)

a. Data Cleaning
  - Databricks connects to Azure Data Lake Gen2 using an Entra ID app
  - Reads Parquet files from the Bronze layer
  - Cleans and normalizes the dataset
  - Writes processed data to the Silver layer

b. Dimensional Modeling
  - Uses widgets to control run type: initial or incremental
  - Creates dimension tables with selected fields and a new DimKey (surrogate key)
  - Checks if a dimension table already exists:
    - If initial run: DimKey starts from 1
    - If incremental: continues from the max DimKey
  - Separates new and existing records
  - Applies Slowly Changing Dimension Type 1 to handle updates
  - Combines records to generate updated dimension tables

c. Fact Table Creation
  - Reads ratings data from the Silver layer
  - Joins dimension keys from dimension tables
  - Drops natural keys from the fact table
  - Writes to the Gold layer:
    - If no existing fact table: overwrite mode
    - If it exists: upsert logic is used
  - Controlled via Databricks Workflows, which pass parameters to notebooks

![image](https://github.com/user-attachments/assets/fd7838b6-043a-4f74-899d-d058286f0014)
Databricks Workflows run screenshot.

3. Data Analytics (Power BI)
- Power BI connects to the Gold layer using the Parquet Connector
- Alternatively, Azure Synapse Analytics can be used to access Gold data
- Visualizations include:
  - Most popular books
  - Best-rated authors
- Sample dashboard available in the PowerBI_Files folder

**GitHub Repo Folder Structure**
- "materials"
  - Python Scripts for data ingestion
  - DAG file for orchestration
  - Docker files for constructing the container
- "databricks_notebooks"
  - Databricks notebooks files for data transformation and dimensional modelling
- "PowerBI_File"
  - Contains an example dashboard showcasing some insights from the data processed
- "README.md"
  - Description of entire project         

![image](https://github.com/user-attachments/assets/8dfbb100-c2a2-4627-b410-316529ccc6a2)
