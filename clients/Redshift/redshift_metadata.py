import pandas as pd
import redshift_connector
import os

host = 'redshift-cluster-e6data.c3mkpxjobn1f.us-east-1.redshift.amazonaws.com'
user = 'awsuser'
query_log_start = '2024-09-10'
query_log_end = '2024-09-24'
password = 'E6data#123'
database = 'dev'
port = 5432
csv_output_dir = '../../redshift-metadata'

os.makedirs(csv_output_dir, exist_ok=True)

# Redshift connection details
conn = redshift_connector.connect(
    host=host,
    database=database,
    user=user,
    password=password
)

# Queries to execute and corresponding CSV file names
queries = {
    'svv_columns.csv': "SELECT * FROM SVV_COLUMNS",
    'svv_tables.csv': "SELECT * FROM SVV_TABLES",
    'svv_table_info.csv': "SELECT * FROM SVV_TABLE_INFO",
    'svv_external_columns.csv': "SELECT * FROM SVV_EXTERNAL_COLUMNS",
    'svv_external_databases.csv': "SELECT * FROM SVV_EXTERNAL_DATABASES",
    'svv_external_partitions.csv': "SELECT * FROM SVV_EXTERNAL_PARTITIONS",
    'svv_external_schemas.csv': "SELECT * FROM SVV_EXTERNAL_SCHEMAS",
    'svv_external_tables.csv': "SELECT * FROM SVV_EXTERNAL_TABLES",
    'pg_library.csv': "SELECT * FROM PG_LIBRARY",
    'pg_database.csv': "SELECT * FROM PG_DATABASE",
    'pg_namespace.csv': "SELECT * FROM PG_NAMESPACE",
    'pg_operator.csv': "SELECT * FROM PG_OPERATOR",
    'pg_tables.csv': "SELECT * FROM PG_TABLES",
    'database.csv': """
        SELECT d.datid as "Database_id", d.datname as "Name", pg_catalog.pg_get_userbyid(d.datdba) as "Owner", 
        pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding", pg_catalog.array_to_string(d.datacl, ',') AS "Access_privileges" 
        FROM pg_catalog.pg_database_info d ORDER BY 1
    """,
    'functions.csv': """
        SELECT n.nspname as "Schema", p.proname as "Name", format_type(p.prorettype, null) as "Result_data_type", 
        oidvectortypes(p.proargtypes) as "Argument_data_types", lang.lanname as "Language_name" 
        FROM pg_catalog.pg_proc p 
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace 
        LEFT JOIN pg_catalog.pg_language lang ON lang.oid = p.prolang 
        WHERE pg_catalog.pg_function_is_visible(p.oid) ORDER BY 1, 2, 4
    """,
    'types.csv': """
        SELECT n.nspname as "Schema", pg_catalog.format_type(t.oid, NULL) AS "Name", 
        pg_catalog.obj_description(t.oid, 'pg_type') as "Description" 
        FROM pg_catalog.pg_type t 
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace 
        WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) 
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem ) 
        AND pg_catalog.pg_type_is_visible(t.oid) ORDER BY 1, 2
    """,
    'aggregates.csv': """
        SELECT n.nspname as "Schema", p.proname AS "Name", pg_catalog.format_type(p.prorettype, NULL) AS "Result_data_type", 
        CASE WHEN p.pronargs = 0 THEN CAST('*' AS pg_catalog.text) ELSE oidvectortypes(p.proargtypes) END AS "Argument_data_types", 
        pg_catalog.obj_description(p.oid, 'pg_proc') as "Description" 
        FROM pg_catalog.pg_proc p 
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace 
        WHERE p.proisagg AND pg_catalog.pg_function_is_visible(p.oid) ORDER BY 1, 2, 4
    """,
    'casts.csv': """
        SELECT pg_catalog.format_type(castsource, NULL) AS "Source_type", pg_catalog.format_type(casttarget, NULL) AS "Target_type", 
        CASE WHEN castfunc = 0 THEN '(binary coercible)' ELSE p.proname END as "Function", 
        CASE WHEN c.castcontext = 'e' THEN 'no' WHEN c.castcontext = 'a' THEN 'in assignment' ELSE 'yes' END as "Implicit?" 
        FROM pg_catalog.pg_cast c 
        LEFT JOIN pg_catalog.pg_proc p ON c.castfunc = p.oid 
        LEFT JOIN pg_catalog.pg_type ts ON c.castsource = ts.oid 
        LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = ts.typnamespace 
        LEFT JOIN pg_catalog.pg_type tt ON c.casttarget = tt.oid 
        LEFT JOIN pg_catalog.pg_namespace nt ON nt.oid = tt.typnamespace 
        WHERE ( (true AND pg_catalog.pg_type_is_visible(ts.oid)) OR (true AND pg_catalog.pg_type_is_visible(tt.oid)) ) 
        ORDER BY 1, 2
    """,
    'is_columns_generic.csv': "SELECT * FROM information_schema.columns WHERE table_schema IN ('pg_catalog', 'pg_internal', 'information_schema')",
    'is_columns_private.csv': "SELECT * FROM information_schema.columns WHERE table_schema NOT IN ('pg_catalog', 'pg_internal', 'information_schema')",
    'pg_table_def_generic.csv': "SELECT * FROM pg_table_def WHERE schemaname IN ('pg_catalog', 'pg_internal', 'information_schema')",
    'pg_table_def_private.csv': "SELECT * FROM pg_table_def WHERE schemaname NOT IN ('pg_catalog', 'pg_internal', 'information_schema')",
    'pg_views_generic.csv': "SELECT * FROM pg_views WHERE schemaname IN ('pg_catalog', 'pg_internal', 'information_schema')",
    'pg_views_private.csv': "SELECT * FROM pg_views WHERE schemaname NOT IN ('pg_catalog', 'pg_internal', 'information_schema')",
    'pg_user.csv': "SELECT * FROM pg_user",
    'stv_mv_info.csv': "SELECT * FROM stv_mv_info",
    'stv_wlm_service_class_config.csv': "SELECT * FROM stv_wlm_service_class_config",
    'stv_wlm_service_class_state.csv': "SELECT * FROM stv_wlm_service_class_state"
}


def run_query_and_save_to_csv(query, csv_filename):
    global cursor
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(result, columns=columns)

        output_path = os.path.join(output_dir, csv_filename)
        df.to_csv(output_path, index=False)

        print(f"Data written to {csv_filename}")
    except Exception as e:
        print(f"Failed to execute query for {csv_filename}: {e}")
    finally:
        cursor.close()


for csv_filename, query in queries.items():
    run_query_and_save_to_csv(query, csv_filename)

conn.close()
