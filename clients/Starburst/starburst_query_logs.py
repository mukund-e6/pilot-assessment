import pandas as pd
from datetime import datetime, timedelta
import os
import trino
from pystarburst import Session
import logging
import pyarrow as pa
import pyarrow.parquet as pq

# logger = logging.getLogger(__name__)
MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_starburst_query_logs(config):
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['password']
    query_log_start = config['query_log_start']
    query_log_end = config['query_log_end']
    catalog = config['catalog']
    schema = config['schema']

    csv_output_dir = 'starburst-query-logs'
    os.makedirs(csv_output_dir, exist_ok=True)
    parquet_output_dir = "starburst_query_logs/starburst-query-logs-parquet"
    os.makedirs(parquet_output_dir, exist_ok=True)
    try:
        db_parameters = {
            "host": host,
            "port": port,
            "http_scheme": "https",

            "auth": trino.auth.BasicAuthentication(user, password)
        }
        session_properties = {
            'catalog': catalog,
            'schema': schema
        }
        session = Session.builder.configs(db_parameters).configs(session_properties).create()
        # print(Session.builder.configs(db_parameters).configs(session_properties).create())
        try:
            start_date = datetime.strptime(query_log_start, '%Y-%m-%d')
            end_date = datetime.strptime(query_log_end, '%Y-%m-%d')

            current_date = start_date
            while current_date <= end_date:
                next_date = current_date + timedelta(days=1)

                start_timestamp = current_date.strftime('%Y-%m-%d')

                history_query = f"""
                                select * from galaxy_telemetry.public.query_history
                                WHERE "date" = '{start_timestamp}'
                                """

                result = session.sql(history_query).collect()

                df = pd.DataFrame(result)

                if df.empty:
                    print(f"No queries were run on {current_date.strftime('%Y-%m-%d')}")
                else:
                    csv_filename = f'{csv_output_dir}/query_history_{current_date.strftime("%Y-%m-%d")}.csv'

                    df.to_csv(csv_filename, index=False)

                    table = pa.Table.from_pandas(df)

                    print(
                        f"Data for {current_date.strftime('%Y-%m-%d')} has been exported to {os.path.basename(csv_filename)}")
                    parquet_filename = f'{parquet_output_dir}/query_history_{current_date.strftime("%Y-%m-%d")}.parquet'
                    pq.write_table(table, parquet_filename)
                # Move to the next day
                current_date = next_date

        except Exception as e:
            print(f"Failed to extract Query Logs: {e}")

        finally:
            session.close()
    except Exception as e:
        logger.error(f"Error extracting Starburst metadata: {str(e)}")