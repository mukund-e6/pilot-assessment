import pandas as pd
import os
import trino
from pystarburst import Session
import logging

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_starburst_metadata(config):
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['password']
    catalog = config['catalog']
    schema = config['schema']

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

                    # Step 2: Fetch tables for each schema
                    metadata_tables_query = f"""SHOW TABLES FROM \"{schema_name}\""""
                    tables_result = session.sql(metadata_tables_query).collect()
                    df_tables = pd.DataFrame(tables_result)

                    if df_tables.empty:
                        print(f"No tables found in schema {schema_name}")
                    else:
                        for table_row in df_tables.itertuples():
                            table_name = table_row[1]

                            # Step 3: Fetch stats for each table
                            metadata_stats_query = f"SHOW STATS FOR \"{schema_name}\".\"{table_name}\""
                            stats_result = session.sql(metadata_stats_query).collect()
                            df_stats = pd.DataFrame(stats_result)

                            if not df_stats.empty:
                                # Append schema, table name to each row in the stats
                                df_stats['schema_name'] = schema_name
                                df_stats['table_name'] = table_name
                                schema_table_list.append(df_stats)

                        # Step 4: Save all tables for the schema into one CSV file
                        if schema_table_list:
                            df_combined_stats = pd.concat(schema_table_list, ignore_index=True)
                            schema_output_dir = os.path.join(csv_output_dir, f'stats_{schema_name}.csv')
                            df_combined_stats.to_csv(schema_output_dir, index=False)
                            print(f"Stats for schema '{schema_name}' saved to {schema_output_dir}")
                        else:
                            print(f"No stats available for schema '{schema_name}'")

        except Exception as e:
            print(f"Failed to extract Query Logs: {e}")

        finally:
            session.close()
    except Exception as e:
        logger.error(f"Error extracting Starburst metadata: {str(e)}")


