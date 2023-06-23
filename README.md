# Retail Sales Workflow
### Description
The dataset for this project is taken from https://www.kaggle.com/code/dgluesen/sales-and-workload-in-retail-industry.<br>
The goals of the project are the following:
- to perform an exploratory data analysis;
- to clean the data incuding handling missing values, fixing data types and joining two tables to a single one;
- to load the data into Postgres database using Prefect orchestrator.
### Project files
- `salesworload.xlsx` - raw data in Excel-based form. More details are available at [dataset source page](https://www.kaggle.com/code/dgluesen/sales-and-workload-in-retail-industry);
- `docker-compose.yaml` - docker containers for Postgres database and PGAdmin client;
- `db.sql` - sql commands to create neccessary databases;
- `EDA_sales_figures.ipynb`, 'EDA_opening_schemes.ipynb' - exploratory analysis notebooks;
- `requirements.txt` - supplementary libraries' list;
- `retail_sales_workflow` - ETL script for data load into database using Prefect.
