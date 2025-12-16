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

    logging.info(
        'Starting CAGED pipeline execution...'
    )

    df_meta_extracted = extract_metadata(spark=spark,
                                         source_path=SHORTCUT_PATH) 
    display(df_meta_extracted.limit(10))
    
    df_metadata = build_metadata(df=df_meta_extracted,
                                 root_path=LANDING_ROOT_PATH)
    display(df_metadata.limit(10))

    df_meta_table = setup_meta_table(spark=spark, 
                                     table_name=LANDING_META_TABLE_NAME)
    display(df_meta_table.limit(10))

    df_ready_for_copy = orchestrate_incremental_copy(df_metadata=df_metadata, 
                                                     df_meta_table=df_meta_table)
    display(df_ready_for_copy.limit(10))

    df_audit, record_count = orchestrate_file_to_copy(spark=spark, 
                                                      df=df_ready_for_copy,
                                                      table_name=LANDING_META_TABLE_NAME)

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