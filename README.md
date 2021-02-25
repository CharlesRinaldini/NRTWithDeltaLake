# NRTWithDeltaLake

## Background
This notebook is an example of how we utilize the Databricks Medallion architecture in our data lakehouse but skip over the bronze zone and go directly to the silver zone for NRT reporting. We then use a similar process to source data from the silver zone to load the bronze zone overnight. This enables us to keep snapshots of our data in bronze but also have it readily available to use for reporting via SQL Analytics. 
Read more here: [NRT with Databricks Delta](https://corgisandcode.com/2021/02/25/near-real-time-ingestion-with-databricks-delta)

## Files
### The notebook for NRT loading and handling of schema evolution
- COPY_MSQL_TO_SILVER.py
### The config database for tracking entities and watermarks
- dbrconfig.sql
- dbrconfigData.sql
### The sample database used to demonstrate incremental loading to delta lake
- dbrdemo.sql
- dbrDemoTransactionsData1.sql
- dbrDemoTransactionsData2.sql
