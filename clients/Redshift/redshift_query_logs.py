import pandas as pd
import redshift_connector
from datetime import datetime, timedelta
import os
import pyarrow as pa
import pyarrow.parquet as pq

host = 'redshift-cluster-1.c3mkpxjobn1f.us-east-1.redshift.amazonaws.com'
user = 'awsuser'
query_log_start = '2024-10-10'
query_log_end = '2024-10-24'
password = 'E6data#123'
database = 'dev'
port = 5432
csv_output_dir = 'redshift-query-logs'
os.makedirs(csv_output_dir, exist_ok=True)
parquet_output_dir = "sf-query-logs/redshift_query_logs_parquet"
os.makedirs(parquet_output_dir, exist_ok=True)


# Redshift connection details
conn = redshift_connector.connect(
    host=host,
    database=database,
    user=user,
    password=password
 )

try:
    cursor = conn.cursor()
    start_date = datetime.strptime(query_log_start, '%Y-%m-%d')
    end_date = datetime.strptime(query_log_end, '%Y-%m-%d')

    # Loop through each day in the range
    current_date = start_date
    while current_date <= end_date:
        next_date = current_date + timedelta(days=1)

        start_timestamp = current_date.strftime('%Y-%m-%d 00:00:00')
        end_timestamp = next_date.strftime('%Y-%m-%d 23:59:59')

        history_query = f"""
                        select * from STL_QUERY 
                            left join STL_QUERY_METRICS on STL_QUERY_METRICS.QUERY=STL_QUERY.QUERY 
                            left join STL_WLM_QUERY on STL_WLM_QUERY.QUERY=STL_QUERY.QUERY 
                            left join STL_PLAN_INFO on STL_PLAN_INFO.QUERY=STL_QUERY.QUERY 
                            left join STL_EXPLAIN on STL_EXPLAIN.QUERY=STL_QUERY.QUERY
                            WHERE STL_QUERY.starttime >= cast('{start_timestamp}' as timestamp)
                            AND STL_QUERY.starttime <= cast('{end_timestamp}' as timestamp);
                        """

        # Execute the query
        cursor.execute(history_query)
        result = cursor.fetchall()

        if len(result) == 0:
            # If no data, print the message and skip file creation
            print(f"No queries were run on {current_date.strftime('%Y-%m-%d')}")
        else:
            # Fetch column names
            columns = [desc[0] for desc in cursor.description]

            # Create a DataFrame from the result
            df = pd.DataFrame(result, columns=columns)

            # Build the CSV file path using the output directory and current date
            csv_filename = f'{csv_output_dir}/query_history_{current_date.strftime("%Y-%m-%d")}.csv'

            # Export the DataFrame to the CSV file
            df.to_csv(csv_filename, index=False)

            # Extract just the file name from the full path
            print(f"Data for {current_date.strftime('%Y-%m-%d')} has been exported to {os.path.basename(csv_filename)}")

            table = pa.Table.from_pandas(df)
            parquet_filename = f'{parquet_output_dir}/query_history_{current_date.strftime("%Y-%m-%d")}.parquet'
            pq.write_table(table, parquet_filename)

        # Move to the next day
        current_date = next_date
    print(f"Query Log Successfully Exported to {csv_output_dir}")

except Exception as e:
    print(f"Failed to extract Query Logs: {e}")

finally:
    cursor.close()
    conn.close()
