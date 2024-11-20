import pandas as pd
import os
import trino
from pystarburst import Session
import logging

# logger = logging.getLogger(__name__)
MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_starburst_metadata_stats(config):
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['password']
    catalog = config['catalog']
    schema = config['schema']
    schema_table_csv = './starburst_metadata/schema_tables_combined.csv'
    csv_output_dir = 'starburst-metadata'
    os.makedirs(csv_output_dir, exist_ok=True)
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
            metadata_schema = f"""
                                SHOW SCHEMAS FROM {catalog}
                            """
            result = session.sql(metadata_schema).collect()
            print(result)
            df_schemas = pd.DataFrame(result)
            schema_table_list = []
            if df_schemas.empty:
                print(f"No Schema Found")
            else:
                for schema_row in df_schemas.itertuples():
                    schema_name = schema_row[1]

                    # Query to get all tables from the current schema
                    metadata_tables_query = f"""SHOW TABLES FROM \"{schema_name}\""""
                    tables_result = session.sql(metadata_tables_query).collect()
                    print(tables_result)
                    df_tables = pd.DataFrame(tables_result)

                    if df_tables.empty:
                        print(f"No tables found in schema {schema_name}")
                    else:
                        for table_row in df_tables.itertuples():
                            table_name = table_row[1]
                            schema_table_list.append([schema_name, table_name])

                df_schema_table = pd.DataFrame(schema_table_list, columns=['Schema', 'Table'])

                combined_csv_filename = f'{csv_output_dir}/schema_tables.csv'
                df_schema_table.to_csv(combined_csv_filename, index=False)
                print(f"Schema saved to {combined_csv_filename}")

        except Exception as e:
            print(f"Failed to extract Query Logs: {e}")

        finally:
            session.close()
    except Exception as e:
        logger.error(f"Error extracting Starburst metadata: {str(e)}")