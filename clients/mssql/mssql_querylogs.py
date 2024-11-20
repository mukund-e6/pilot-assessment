import pandas as pd
import pymssql
from datetime import datetime, timedelta
import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def extract_query_logs(config):
    server = config['server'],
    user = config['user'],
    password = config['password'],
    database = config['database'],
    port = config['port'],
    query_log_start = config['query_log_start'],
    query_log_end = config['query_log_end'],
    csv_output_dir = 'mssql-query-logs'
    os.makedirs(csv_output_dir, exist_ok=True)
    parquet_output_dir = "mssql-query-logs/mssql-query-logs-parquet"
    os.makedirs(parquet_output_dir, exist_ok=True)

    try:
        conn = pymssql.connect(
            server=server[0],
            user=user[0],
            password=password[0],
            database=database[0],
            port=port[0],
            as_dict=True
        )
        try:
            cursor = conn.cursor()
            start_date = datetime.strptime(query_log_start[0], '%Y-%m-%d')
            end_date = datetime.strptime(query_log_end[0], '%Y-%m-%d')

            # Loop through each day in the range
            current_date = start_date
            while current_date <= end_date:
                next_date = current_date + timedelta(days=1)  # Move to the next day

                # Format dates for SQL query
                start_timestamp = current_date.strftime('%Y-%m-%d 00:00:00')
                end_timestamp = next_date.strftime('%Y-%m-%d 23:59:59')

                # Query to fetch logs for the current day
                history_query = f"""
                                SELECT * FROM sys.dm_exec_query_stats AS qs
                                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
                                WHERE qs.creation_time >= '{start_timestamp}'
                                AND qs.creation_time <= '{end_timestamp}';
                                """

                cursor.execute(history_query)
                rows = cursor.fetchall()

                columns = [column[0] for column in cursor.description]

                df = pd.DataFrame.from_records(rows, columns=columns)

                if df.empty:
                    print(f"No queries were run on {current_date.strftime('%Y-%m-%d')}")
                else:
                    csv_filename = f'{csv_output_dir}/query_history_{current_date.strftime("%Y-%m-%d")}.csv'
                    table = pa.Table.from_pandas(df)
                    df.to_csv(csv_filename, index=False)

                    csv_filename_only = os.path.basename(csv_filename)
                    print(f"Data for {current_date.strftime('%Y-%m-%d')} has been exported to {csv_filename_only}")

                    parquet_filename = f'{parquet_output_dir}/query_history_{current_date.strftime("%Y-%m-%d")}.parquet'
                    pq.write_table(table, parquet_filename)
                    print(f"Data for {current_date.strftime('%Y-%m-%d')} has been exported to {parquet_filename}")

                current_date = next_date

        except Exception as e:
            print(f"Failed to extract Query Logs: {e}")

        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error extracting Snowflake metadata: {str(e)}")
