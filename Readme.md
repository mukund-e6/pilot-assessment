# e6data Migration Assessment Tool

### Migration Assessment Overview

The Migration Assessment tool enables the extraction of metadata and query history (including query metrics) from a existing data
warehouse for analysis.

To run the migration assessment, clone the repo the e6-migration-assessment tool from:
```commandline
git clone 
```

With the help of Migration Assessment you can analyse:

```
Metadata: database name, database size, table name, table size, row count, partition info, external databases/tables info
Query Logs: query text, query plan, query metrics ( execution time, planning time, byte scanned, etc.)
```
```
main.py should be inside the repo of the tool e6-migration-assessment
```
```
Path Example: /users/e6-migration-assessment/main.py
```
Steps to Run the Script:

```
- Pre Assessment Requirements:
- e6-migration-assessment tool for running the assessment
- Databricks Permission to view system.information_schema
- Python 3.9 or above
- Databricks SQL Python connector
pip install databricks-sql-connector
```



Extraction of the Metadata and Query Logs:
After cloning the e6-migration-assessment tool, Follow below steps to extract metadata and query logs.
```
cd <PATH>/e6-migration-assessment
```
Run the following export commands pertaining to your databricks configurations (host, access_token, warehouse_id, etc.)
```
export DBR_HOST="dbc-xxx-yyy"
export DBR_WAREHOUSE_ID="8123456789"
export DBR_ACCESS_TOKEN="dapi12345abcdef"
export query_log_start='202Y-MM-DD' (Example 2024-10-11)
export query_log_end='202Y-MM-DD' (Example 2024-10-15)
```

```
Edit the above export commands and run through terminal
```
### To run the Query Log extractor
```
python3 main.py databricks querylogs
```

### To run the Metadata extractor
```
python3 main.py databricks metadata
```

Two directories named databricks-metadata for Metadata and databricks-query-logs for Query Logs will be generated with the help of above scripts
in the assessment-dump directory.
Share both the directories in zip format with e6data for assessments.