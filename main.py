import logging
from IPython.display import display
from pyspark.sql import SparkSession

from config.paths import (SHORTCUT_PATH, 
                          LANDING_ROOT_PATH,
                          LANDING_META_TABLE_NAME)

from src.landing.extract_metadata import extract_metadata
from landing.build_metadata import build_metadata
from landing.setup_metadata_table import setup_meta_table
from landing.orchestrate_incremental_copy import orchestrate_incremental_copy
from landing.orchestrate_files_to_copy import orchestrate_file_to_copy
from landing.write_audit_table import write_audit_table

def main(): 
    
    spark = (
    SparkSession.builder
        .appName('Caged-pipeline')
        .getOrCreate())

    try: 
        logging.info(
            'Starting CAGED pipeline execution...'
        )
        logging.info(
            'Step 1/6: Extracting metadata from source files (S3 via OneLake Shortcut).'
        )
        df_meta_extracted = extract_metadata(spark=spark,
                                             source_path=SHORTCUT_PATH) 
        display(df_meta_extracted.limit(10))
        
        logging.info(
            'Step 2/6: Building relative paths and selecting the final set of metadata column.'
        )
        df_metadata = build_metadata(df=df_meta_extracted,
                                     root_path=LANDING_ROOT_PATH)
        display(df_metadata.limit(10))

        logging.info(
            'Step 3/6: Define the schema, then attempts to create the table and returns the table as a Spark DataFrame.'
        )
        df_meta_table = setup_meta_table(spark=spark, 
                                         table_name=LANDING_META_TABLE_NAME)
        display(df_meta_table.limit(10))

        logging.info(
            'Step 4/6: Identifying new or updated files for incremental ingestion'
        )
        df_ready_for_copy = orchestrate_incremental_copy(df_metadata=df_metadata, 
                                                         df_meta_table=df_meta_table)
        display(df_ready_for_copy.limit(10))

        logging.info(
            'Step 5/6: Executing distributed file copy from source to Lakehouse and generating audit records'
        )
        df_audit, record_count = orchestrate_file_to_copy(spark=spark, 
                                                          df=df_ready_for_copy,
                                                          table_name=LANDING_META_TABLE_NAME)
        logging.info(
            'Step 6/6: Loads the audit records from the DataFrame to the Delta table'
        )
        write_audit_table(df=df_audit,
                          table_name=LANDING_META_TABLE_NAME,
                          count=record_count)
        
        logging.info(
            'CAGED pipeline execution completed successfully.'
        )
        return (df_meta_extracted,
                df_metadata,
                df_meta_table,
                df_ready_for_copy,
                df_audit)
    
    except Exception as e:
        logging.error(
            f'FATAL ERROR: CAGED pipeline execution FAILED. Shutting down.',
            exc_info=True
        )
        raise 