# e6data Migration Assessment Tool
The Migration Assessment tool enables the extraction of metadata and query history (including query metrics) from a existing data
warehouse for further analysis.
```
Metadata: database name, database size, table name, table size, row count, partition info, external databases/tables info
Query Logs: query text, query plan, query metrics ( execution time, planning time, byte scanned, etc.)
```
### Steps to Run the Script:

#### Databricks
```
Pre Assessment Requirements:

- Databricks Permission to view system.information_schema
- Python 3.9 or above
- Pandas, Databricks SQL Python connector
```
```
pip install pandas
pip install databricks-sql-connector
```
#### Snowflake
```
Pre Assessment Requirements:

- e6-migration-assessment tool for running the assessment
- Snowflake role to view query history and information schema (ACCOUNT_ADMIN preferred)
- Python 3.9 or above
- Pandas, Snowflake SQL Python connector
```
```
pip install pandas
pip install snowflake-connector-python
```


### Extraction of the Metadata and Query Logs:
#### Databricks
Run the following export commands pertaining to your databricks configurations (host, access_token, warehouse_id, etc.)
```
export DBR_HOST=<databricks_host>
export DBR_WAREHOUSE_ID=<warehouse-id>
export DBR_ACCESS_TOKEN=<databrciks_token>
export query_log_start='YYYY-MM-DD' (Example 2024-10-11)
export query_log_end='YYYY-MM-DD' (Example 2024-10-15)
```
To run the assessment script :
```
python3 client/main.py databricks
```
#### Snowflake
Run the following export commands pertaining to your snowflake configurations (host, warehouse, role, user, password, etc.)
```
export SNOWFLAKE_HOST=<snowflake_host>
export SNOWFLAKE_WAREHOUSE=<warehouse_name>
export SNOWFLAKE_ROLE='ACCOUNTADMIN'
export SNOWFLAKE_USER=<snowflake_username>
export SNOWFLAKE_PASSWORD=<snowflake_password>
export QUERY_LOG_START='YYYY-MM-DD'
export QUERY_LOG_END='YYYY-MM-DD'
```
To run the assessment script :
```
python3 client/main.py snowflake
```

Two directories named <client>-metadata for Metadata and <client>-query-logs for Query Logs will be generated with the help of above script.